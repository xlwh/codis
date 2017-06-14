// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"
	"sync"

	"github.com/ksarch-saas/codis/pkg/models"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
)

const MaxSlotNum = models.DEFAULT_SLOT_NUM

type Router struct {
	mu sync.Mutex

	auth string
	idcs []string
	pool map[string]*SharedBackendConn

	slots [MaxSlotNum]*Slot

	closed bool
}

func New(idcs []string) *Router {
	return NewWithAuth("", idcs)
}

func NewWithAuth(auth string, idcs []string) *Router {
	s := &Router{
		auth: auth,
		pool: make(map[string]*SharedBackendConn),
		idcs: idcs,
	}
	for i := 0; i < len(s.slots); i++ {
		s.slots[i] = &Slot{id: i}
	}
	return s
}

func (s *Router) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for i := 0; i < len(s.slots); i++ {
		s.resetSlot(i)
	}
	s.closed = true
	return nil
}

var errClosedRouter = errors.New("use of closed router")

func (s *Router) ResetSlot(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.resetSlot(i)
	return nil
}

func (s *Router) FillSlot(i int, servers []*models.Server, from string, lock bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.fillSlot(i, servers, from, lock)
	return nil
}

func (s *Router) KeepAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	for _, bc := range s.pool {
		bc.KeepAlive()
	}
	return nil
}

func (s *Router) Dispatch(r *Request) error {
	hkey := getHashKey(r.Resp, r.OpStr)
	slot := s.slots[hashSlot(hkey)]
	return slot.forward(r, hkey, s.idcs)
}

func (s *Router) DispatchAsking(r *Request, hkey []byte) error {
	idx := hashSlot(hkey)
	slot := s.slots[idx]
	return slot.forwardAsk(r, hkey)
}

func (s *Router) getBackendConn(addr string) *SharedBackendConn {
	bc := s.pool[addr]
	if bc != nil {
		bc.IncrRefcnt()
	} else {
		bc = NewSharedBackendConn(addr, s.auth)
		s.pool[addr] = bc
	}
	return bc
}

func (s *Router) putBackendConn(bc *SharedBackendConn) {
	if bc != nil && bc.Close() {
		delete(s.pool, bc.Addr())
	}
}

func (s *Router) isValidSlot(i int) bool {
	return i >= 0 && i < len(s.slots)
}

func (s *Router) resetSlot(i int) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait()

	for _, b := range slot.backends {
		s.putBackendConn(b.bc)
	}
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	slot.unblock()
}

func (s *Router) fillSlot(i int, servers []*models.Server, from string, lock bool) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait()

	for _, b := range slot.backends {
		s.putBackendConn(b.bc)
	}
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	for _, serv := range servers {
		if len(serv.Addr) != 0 {
			xx := strings.Split(serv.Addr, ":")
			b := &backend{}
			if len(xx) >= 1 {
				b.host = []byte(xx[0])
			}
			if len(xx) >= 2 {
				b.port = []byte(xx[1])
			}

			/* check if is availiable server */
			flag := false
			for _, tag := range s.idcs {
				if serv.Tag == tag {
					flag = true
					break
				}
			}
			if !flag && serv.Type != models.SERVER_TYPE_MASTER {
				continue
			}

			/* skip offline and disable */
			if serv.Type == models.SERVER_TYPE_OFFLINE || serv.Type == models.SERVER_TYPE_DISABLE {
				continue
			}
			b.addr = serv.Addr
			b.bc = s.getBackendConn(b.addr)
			b.tag = serv.Tag
			b.gid = serv.GroupId
			b.role = serv.Type
			if b.role == models.SERVER_TYPE_MASTER {
				slot.master = b
			}
			slot.backends = append(slot.backends, b)
		}
	}

	if len(from) != 0 {
		slot.migrate.from = from
		slot.migrate.bc = s.getBackendConn(from)
	}

	if !lock {
		slot.unblock()
	}

	if slot.migrate.bc != nil {
		log.Debugf("fill slot %04d, backend.addr = %s, migrate.from = %s",
			i, slot.master.addr, slot.migrate.from)
	} else {
		log.Debugf("fill slot %05d to %s in group %d", i, slot.master.addr, slot.master.gid)
	}
}
