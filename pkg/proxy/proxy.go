package proxy

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ksarch-saas/codis/pkg/models"
	"github.com/ksarch-saas/codis/pkg/proxy/router"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
)

type Proxy struct {
	mu     sync.Mutex
	closed bool

	init, exit struct {
		C chan struct{}
	}

	lproxy net.Listener
	config *Config
	router *router.Router
	topo   *Topo
}

var ErrCloseProxy = errors.New("use of closed proxy")

func New(addr, debugVarAddr string, config *Config) (*Proxy, error) {
	config.proxyAddr = addr
	s := &Proxy{config: config}
	s.init.C = make(chan struct{})
	s.exit.C = make(chan struct{})

	if err := s.setup(); err != nil {
		s.Close()
		return nil, err
	}

	go s.polling()
	go s.serve()
	go s.keepAlive(config.keepAlivePeriod)
	return s, nil
}

func (s *Proxy) Run() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrCloseProxy
	}
	s.topo.Run()
	s.initMeta()
	return nil
}

func (s *Proxy) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)
	if s.lproxy != nil {
		s.lproxy.Close()
	}
	if s.router != nil {
		s.router.Close()
	}
	s.topo.Close()
	return nil
}

func (s *Proxy) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Proxy) setup() error {
	l, err := net.Listen("tcp", s.config.proxyAddr)
	if err != nil {
		return errors.Trace(err)
	}
	s.lproxy = l
	var idcs []string
	idcs = append(idcs, s.config.tag)
	idcs = append(idcs, s.config.backup[s.config.tag]...)
	s.router = router.New(idcs)
	addrs := strings.Split(s.config.metaAddrs, ",")
	s.topo = NewTopo(s.config.productName, addrs, s.config.pollingInterval)
	return nil
}

func (s *Proxy) fillSlot(i int, m *models.Meta) error {
	var slot *models.Slot
	var group *models.ServerGroup
	var ok bool
	slot, ok = m.Slots[fmt.Sprintf("%d", i)]
	if !ok {
		return fmt.Errorf("can't find slot - %d", i)
	}

	group, ok = m.Groups[fmt.Sprintf("%d", slot.GroupId)]
	if !ok {
		return fmt.Errorf("can't find group - %d", slot.GroupId)
	}

	var from string
	addr, err := groupMaster(group)
	if err != nil {
		return errors.Trace(err)
	}
	if slot.State.Status == models.SLOT_STATUS_MIGRATE {
		fromGroup, ok := m.Groups[fmt.Sprintf("%d", slot.State.MigrateStatus.From)]
		if !ok {
			return fmt.Errorf("can't find migrate group - %d", slot.State.MigrateStatus.From)
		}
		from, err = groupMaster(fromGroup)
		if err != nil {
			return errors.Trace(err)
		}
		if from == addr {
			return fmt.Errorf("set slot %04d migrate from %s to %s", i, from, addr)
		}
	}
	s.router.FillSlot(i, group.Servers, from, false)
	return nil
}

func (s *Proxy) fillSlots() {
	m := <-s.topo.Notify()
	for i := 0; i < models.DEFAULT_SLOT_NUM; i++ {
		if err := s.fillSlot(i, m); err != nil {
			log.PanicErrorf(err, "fill slot %d failed", i)
		}
	}
}

func (s *Proxy) initMeta() {
	s.fillSlots()
	log.Info("meta init done")
	close(s.init.C)
}

func (s *Proxy) polling() {
	for {
		s.fillSlots()
	}
}

func (s *Proxy) serve() {
	select {
	case <-s.exit.C:
		return
	case <-s.init.C:
	}

	ch := make(chan net.Conn, 4096)
	go func() {
		for c := range ch {
			s.newSession(c)
		}
	}()

	eh := make(chan error, 1)
	go func(l net.Listener) (err error) {
		defer func() {
			eh <- err
			close(ch)
		}()
		for {
			c, err := s.acceptConn(l)
			if err != nil {
				return err
			}
			ch <- c
		}
	}(s.lproxy)

	select {
	case <-s.exit.C:
		log.Warnf("[%p] proxy shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] proxy exit on error", s)
	}
}

func (s *Proxy) keepAlive(seconds int) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		for i := 0; i < seconds; i++ {
			select {
			case <-s.exit.C:
				return
			case <-ticker.C:
			}
		}
		s.router.KeepAlive()
	}
}

func (s *Proxy) newSession(c net.Conn) {
	x := router.NewSessionSize(c, "", s.config.maxBufSize, s.config.maxTimeout, s.config.slowThreshold)
	x.SetKeepAlive(true)
	x.SetKeepAlivePeriod(time.Second * time.Duration(s.config.keepAlivePeriod))
	go x.Serve(s.router, s.config.maxPipeline)
}

func (s *Proxy) acceptConn(l net.Listener) (net.Conn, error) {
	for {
		c, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed", s)
				time.Sleep(time.Millisecond * 10)
				continue
			}
		}
		return c, err
	}
}

func (s *Proxy) Config() *Config {
	return s.config
}

func (s *Proxy) Info() string {
	return ""
}

func groupMaster(g *models.ServerGroup) (string, error) {
	var master string
	for _, s := range g.Servers {
		if s.Type == models.SERVER_TYPE_MASTER {
			if master != "" {
				return "", fmt.Errorf("duplicated master in group %d", g.Id)
			}
			master = s.Addr
		}
	}
	if master == "" {
		return "", fmt.Errorf("no master found in group %d", g.Id)
	}
	return master, nil
}
