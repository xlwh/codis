// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ksarch-saas/codis/pkg/proxy/redis"
	"github.com/ksarch-saas/codis/pkg/utils/atomic2"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
)

type Session struct {
	*redis.Conn

	Ops int64

	LastOpUnix int64
	CreateUnix int64

	auth       string
	authorized bool

	dispatcher *Dispatcher
	tasks      chan<- *Request

	quit   bool
	failed atomic2.Bool
	closed atomic2.Bool

	/* slow threshold */
	slowThreshold int
}

func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`
		LastOpUnix int64  `json:"lastop"`
		CreateUnix int64  `json:"create"`
		RemoteAddr string `json:"remote"`
	}{
		s.Ops, s.LastOpUnix, s.CreateUnix,
		s.Conn.Sock.RemoteAddr().String(),
	}
	b, _ := json.Marshal(o)
	return string(b)
}

func NewSession(c net.Conn, auth string, slowThreshold int) *Session {
	return NewSessionSize(c, auth, 1024*1024, 1800, slowThreshold)
}

func NewSessionSize(c net.Conn, auth string, bufsize int, timeout int, slowThreshold int) *Session {
	s := &Session{CreateUnix: time.Now().Unix(), auth: auth, slowThreshold: slowThreshold}
	s.Conn = redis.NewConnSize(c, bufsize)
	s.Conn.ReaderTimeout = time.Second * time.Duration(timeout)
	s.Conn.WriterTimeout = time.Second * 30
	log.Debugf("session [%p] create: %s", s, s)
	return s
}

func (s *Session) Close() error {
	s.failed.Set(true)
	s.closed.Set(true)
	return s.Conn.Close()
}

func (s *Session) IsClosed() bool {
	return s.closed.Get()
}

func (s *Session) SetDispather(d Dispatcher) {
	s.dispatcher = &d
}

func (s *Session) GetDispather() Dispatcher {
	return *(s.dispatcher)
}

func (s *Session) SetTasks(tasks chan<- *Request) {
	s.tasks = tasks
}

func (s *Session) GetTasks() chan<- *Request {
	return s.tasks
}

func (s *Session) Serve(d Dispatcher, maxPipeline int) {
	if d == nil {
		log.Infof("dispatcher is nil")
		return
	}
	// set dispacher for this session
	s.SetDispather(d)

	var errlist errors.ErrorList
	defer func() {
		if err := errlist.First(); err != nil {
			log.Debugf("session [%p] closed: %s, error = %s", s, s, err)
		} else {
			log.Debugf("session [%p] closed: %s, quit", s, s)
		}
	}()

	tasks := make(chan *Request, maxPipeline)
	s.SetTasks(tasks)

	go func() {
		defer func() {
			s.Close()
			for _ = range tasks {
			}
		}()
		if err := s.loopWriter(tasks); err != nil {
			errlist.PushBack(err)
		}
	}()

	defer func(tasks chan<- *Request) {
		close(tasks)
		s.SetTasks(nil)
	}(tasks)
	if err := s.loopReader(tasks); err != nil {
		errlist.PushBack(err)
	}
}

func (s *Session) loopReader(tasks chan<- *Request) error {
	for !s.quit {
		resp, err := s.Reader.Decode()
		if err != nil {
			incrClientErrors()
			return err
		}
		r, err := s.handleRequest(resp)
		if err != nil {
			incrForwardErrors()
			return err
		} else {
			tasks <- r
		}
	}
	return nil
}

func (s *Session) loopWriter(tasks <-chan *Request) error {
	p := &FlushPolicy{
		Encoder:     s.Writer,
		MaxBuffered: 32,
		MaxInterval: 300,
	}
	for r := range tasks {
		resp, err := s.handleResponse(r)
		if err != nil {
			return err
		}

		log.Debugf("receive a response %s", string(r.Response.Resp.Value))
		// 当resp返回为ASK，需要重新请求后端
		if r.isAskResp() {
			log.Debugf("ASK: handle -ASK req:response %s:%s", string(r.Resp.Value), string(r.Response.Resp.Value))
			if _, err = s.handleAskRequest(r); err != nil {
				log.Debugf("ASK: handleAskRequest error %s", err)
				return err
			}
			// 重新处理请求的返回
			log.Debugf("ASK: receive new response")
			resp, err = s.handleResponse(r)
			log.Debugf("ASK: new response %s", string(r.Response.Resp.Value))
		}
		// 如果该请求为proxy主动发送的请求，将response swallow
		if r.isAskingReq() {
			return nil
		}
		// 如果该请求为PONG，将response swallow
		if r.isPongResp() {
			return nil
		}

		if err = p.Encode(resp, len(tasks) == 0); err != nil {
			return err
		}
	}
	return nil
}

var ErrRespIsRequired = errors.New("resp is required")

func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
	r.Wait.Wait()
	if r.Coalesce != nil {
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	resp, err := r.Response.Resp, r.Response.Err
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrRespIsRequired
	}
	costMicroseconds := microseconds() - r.Start
	costMilliSeconds := costMicroseconds / 1000
	incrOpStats(r.OpStr, costMicroseconds)

	key := getHashKey(r.Resp, string(r.OpStr))
	/* slow log */
	if costMilliSeconds > int64(s.slowThreshold) {
		log.Slowf("request slow, method: %s key: %s cost: %v ms  target: %s", r.OpStr, key, costMilliSeconds, r.Target)
	}

	log.Debugf("request done, method: %s key: %s cost: %v ms target: %s", r.OpStr, key, costMicroseconds, r.Target)
	return resp, nil
}

/*
* 对后端返回的ASK，重新向正确的后端发送
* r : 已经返回ASK的请求
 */
func (s *Session) handleAskRequest(r *Request) (*Request, error) {
	// reset response
	r.Response.Resp, r.Response.Err = nil, nil

	usnow := microseconds()
	s.LastOpUnix = usnow / 1e6
	s.Ops++

	// get hash key of the request
	hkey := getHashKey(r.Resp, r.OpStr)
	log.Debugf("ASK: use key: %s to choose backend", string(hkey))

	// create a asking request to backend
	nr := &Request{
		OpStr:  "ASKING",
		Start:  usnow,
		Resp:   redis.NewString([]byte("ASKING")),
		Wait:   &sync.WaitGroup{},
		Failed: &s.failed,
	}
	nr, err := s.handleRequestAsking(nr, hkey)
	log.Debugf("ASK: after dispatch asking request, waiting. error=%s", err)
	if err != nil {
		return r, err
	} else {
		s.GetTasks() <- nr
	}

	// resend the r to backend
	err = s.GetDispather().Dispatch(r)
	if err == nil {
		s.GetTasks() <- r
	}
	return r, err
}

func (s *Session) handleRequest(resp *redis.Resp) (*Request, error) {
	opstr, err := getOpStr(resp)
	if err != nil {
		return nil, err
	}
	if isNotAllowed(opstr) {
		return nil, errors.New(fmt.Sprintf("command <%s> is not allowed", opstr))
	}

	l, w := opCheck(opstr)
	if !l {
		return nil, errors.New(fmt.Sprintf("unknown command <%s>", opstr))
	}

	usnow := microseconds()
	s.LastOpUnix = usnow / 1e6
	s.Ops++

	r := &Request{
		OpStr:  opstr,
		Start:  usnow,
		Resp:   resp,
		Wait:   &sync.WaitGroup{},
		Failed: &s.failed,
		Write:  w,
	}

	if opstr == "QUIT" {
		return s.handleQuit(r)
	}
	if opstr == "AUTH" {
		return s.handleAuth(r)
	}

	if !s.authorized {
		if s.auth != "" {
			r.Response.Resp = redis.NewError([]byte("NOAUTH Authentication required."))
			return r, nil
		}
		s.authorized = true
	}

	switch opstr {
	case "SELECT":
		return s.handleSelect(r)
	case "PING":
		return s.handlePing(r)
	case "MGET":
		return s.handleRequestMGet(r)
	case "MSET":
		return s.handleRequestMSet(r)
	case "DEL":
		return s.handleRequestMDel(r)
	}
	return r, s.GetDispather().Dispatch(r)
}

func (s *Session) handleQuit(r *Request) (*Request, error) {
	s.quit = true
	r.Response.Resp = redis.NewString([]byte("OK"))
	return r, nil
}

func (s *Session) handleAuth(r *Request) (*Request, error) {
	if len(r.Resp.Array) != 2 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'AUTH' command"))
		return r, nil
	}
	if s.auth == "" {
		r.Response.Resp = redis.NewError([]byte("ERR Client sent AUTH, but no password is set"))
		return r, nil
	}
	if s.auth != string(r.Resp.Array[1].Value) {
		s.authorized = false
		r.Response.Resp = redis.NewError([]byte("ERR invalid password"))
		return r, nil
	} else {
		s.authorized = true
		r.Response.Resp = redis.NewString([]byte("OK"))
		return r, nil
	}
}

func (s *Session) handleSelect(r *Request) (*Request, error) {
	if len(r.Resp.Array) != 2 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'SELECT' command"))
		return r, nil
	}
	if db, err := strconv.Atoi(string(r.Resp.Array[1].Value)); err != nil {
		r.Response.Resp = redis.NewError([]byte("ERR invalid DB index"))
		return r, nil
	} else if db != 0 {
		r.Response.Resp = redis.NewError([]byte("ERR invalid DB index, only accept DB 0"))
		return r, nil
	} else {
		r.Response.Resp = redis.NewString([]byte("OK"))
		return r, nil
	}
}

func (s *Session) handlePing(r *Request) (*Request, error) {
	if len(r.Resp.Array) != 1 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'PING' command"))
		return r, nil
	}
	r.Response.Resp = redis.NewString([]byte("PONG"))
	return r, nil
}

func (s *Session) handleRequestMGet(r *Request) (*Request, error) {
	nkeys := len(r.Resp.Array) - 1
	if nkeys <= 1 {
		return r, s.GetDispather().Dispatch(r)
	}
	var sub = make([]*Request, nkeys)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i+1],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := s.GetDispather().Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	r.Coalesce = func() error {
		var array = make([]*redis.Resp, len(sub))
		for i, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsArray() || len(resp.Array) != 1 {
				return errors.New(fmt.Sprintf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array)))
			}
			array[i] = resp.Array[0]
		}
		r.Response.Resp = redis.NewArray(array)
		return nil
	}
	return r, nil
}

func (s *Session) handleRequestMSet(r *Request) (*Request, error) {
	nblks := len(r.Resp.Array) - 1
	if nblks <= 2 {
		return r, s.GetDispather().Dispatch(r)
	}
	if nblks%2 != 0 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'MSET' command"))
		return r, nil
	}
	var sub = make([]*Request, nblks/2)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i*2+1],
				r.Resp.Array[i*2+2],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := s.GetDispather().Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	r.Coalesce = func() error {
		for _, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsString() {
				return errors.New(fmt.Sprintf("bad mset resp: %s value.len = %d", resp.Type, len(resp.Value)))
			}
			r.Response.Resp = resp
		}
		return nil
	}
	return r, nil
}

func (s *Session) handleRequestMDel(r *Request) (*Request, error) {
	nkeys := len(r.Resp.Array) - 1
	if nkeys <= 1 {
		return r, s.GetDispather().Dispatch(r)
	}
	var sub = make([]*Request, nkeys)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i+1],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := s.GetDispather().Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	r.Coalesce = func() error {
		var n int
		for _, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsInt() || len(resp.Value) != 1 {
				return errors.New(fmt.Sprintf("bad mdel resp: %s value.len = %d", resp.Type, len(resp.Value)))
			}
			if resp.Value[0] != '0' {
				n++
			}
		}
		r.Response.Resp = redis.NewInt([]byte(strconv.Itoa(n)))
		return nil
	}
	return r, nil
}

func (s *Session) handleRequestAsking(r *Request, extra []byte) (*Request, error) {
	log.Debugf("ASK: handling asking request to backend")
	if err := s.GetDispather().DispatchAsking(r, extra); err != nil {
		return nil, err
	}
	return r, nil
}

func microseconds() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}
