// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	//"fmt"
	"math/rand"
	"sync"

	//"github.com/ksarch-saas/codis/pkg/proxy/redis"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
)

type backend struct {
	addr string
	host []byte
	port []byte
	tag  string
	role string
	gid  int
	bc   *SharedBackendConn
}

type Slot struct {
	id int

	backends []*backend
	master   *backend
	migrate  struct {
		from string
		bc   *SharedBackendConn
	}

	wait sync.WaitGroup
	lock struct {
		hold bool
		sync.RWMutex
	}
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock()
	}
	s.wait.Wait()
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
}

func (s *Slot) reset() {
	s.backends = s.backends[:0]
	/*s.backend.addr = ""
	s.backend.host = nil
	s.backend.port = nil
	s.backend.bc = nil*/
	s.migrate.from = ""
	s.migrate.bc = nil
	s.master = nil
}

func (s *Slot) forward(r *Request, key []byte, idcs []string) error {
	s.lock.RLock()
	bc, err := s.prepare(r, key, idcs)
	s.lock.RUnlock()
	r.Target = bc.addr
	log.Debugf("forward to slot %d server: %s", s.id, bc.addr)
	if err != nil {
		return err
	} else {
		bc.PushBack(r)
		return nil
	}
}

func (s *Slot) forwardAsk(r *Request, key []byte) error {
	s.lock.RLock()
	bc, err := s.prepareAsk(r)
	s.lock.RUnlock()
	if err != nil {
		return err
	} else {
		bc.PushBack(r)
		return nil
	}
}

var ErrSlotIsNotMigrating = errors.New("slot is not in migrating")

func (s *Slot) prepareAsk(r *Request) (*SharedBackendConn, error) {
	if s.migrate.bc == nil {
		log.Debugf("ASK: slot-%04d is not migrating", s.id)
		return nil, ErrSlotIsNotMigrating
	}
	r.slot = &s.wait
	r.slot.Add(1)
	return s.migrate.bc, nil
}

var ErrSlotIsNotReady = errors.New("slot is not ready, may be offline")
var ErrSlotNoResource = errors.New("no available resource")

func (s *Slot) getBackendConn(write bool, idcs []string) *SharedBackendConn {
	if write {
		return s.master.bc
	}
	size := len(s.backends)
	id := rand.Intn(size)
	for _, tag := range idcs {
		for i := 0; i < size; i++ {
			id = (id + 1) % size
			if s.backends[id].tag == tag {
				log.Debugf("select tag: %s", tag)
				return s.backends[id].bc
			}
		}
	}

	/* no available readonly backend found, return master instead */
	log.Warnf("no available readonly backend found slot-%04d for tag %s", s.id, idcs[0])
	return s.master.bc
}

func (s *Slot) prepare(r *Request, key []byte, idcs []string) (*SharedBackendConn, error) {
	if len(s.backends) == 0 {
		log.Warnf("slot-%04d is not ready: key = %s", s.id, key)
		return nil, ErrSlotIsNotReady
	}
	bc := s.getBackendConn(r.Write, idcs)
	if bc == nil {
		log.Warnf("no available found slot-%04d for tag %s", s.id, idcs[0])
		return nil, ErrSlotNoResource
	}
	/* disable migration in proxy as blocking request */
	r.slot = &s.wait
	r.slot.Add(1)
	return bc, nil
	/*if err := s.slotsmgrt(r, key); err != nil {
		log.Warnf("slot-%04d migrate from = %s to %s failed: key = %s, error = %s",
			s.id, s.migrate.from, s.backend.addr, key, err)
		return nil, err
	} else {
		r.slot = &s.wait
		r.slot.Add(1)
		return s.backend.bc, nil
	}*/
}

/*func (s *Slot) slotsmgrt(r *Request, key []byte) error {
	if len(key) == 0 || s.migrate.bc == nil {
		return nil
	}
	m := &Request{
		Resp: redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("SLOTSMGRTTAGONE")),
			redis.NewBulkBytes(s.backend.host),
			redis.NewBulkBytes(s.backend.port),
			redis.NewBulkBytes([]byte("3000")),
			redis.NewBulkBytes(key),
		}),
		Wait: &sync.WaitGroup{},
	}
	s.migrate.bc.PushBack(m)

	m.Wait.Wait()

	resp, err := m.Response.Resp, m.Response.Err
	if err != nil {
		return err
	}
	if resp == nil {
		return ErrRespIsRequired
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	}
	if resp.IsInt() {
		log.Debugf("slot-%04d migrate from %s to %s: key = %s, resp = %s",
			s.id, s.migrate.from, s.backend.addr, key, resp.Value)
		return nil
	} else {
		return errors.New(fmt.Sprintf("error resp: should be integer, but got %s", resp.Type))
	}
}*/
