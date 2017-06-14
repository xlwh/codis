package meta

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"gitlab.baidu.com/go/glog"

	"github.com/ksarch-saas/codis/pkg/models"
	"github.com/ksarch-saas/codis/pkg/utils"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/wandoulabs/zkhelper"
)

const (
	EPIC_SIZE = 8
)

type MetaServer struct {
	addr    string
	zkhost  string
	product string
	meta    *models.Meta
	ln      net.Listener
	evtbus  chan interface{}
	init    chan struct{}

	mu sync.RWMutex
}

func NewMetaServer(addr, product, zkhost string) *MetaServer {
	return &MetaServer{
		addr:    addr,
		zkhost:  zkhost,
		product: product,
		meta:    &models.Meta{Epic: 0},
		evtbus:  make(chan interface{}),
		init:    make(chan struct{}),
	}
}

func (m *MetaServer) collect() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group_dic := make(map[int]bool)
	data := &models.Meta{
		Slots:   make(map[string]*models.Slot),
		Groups:  make(map[string]*models.ServerGroup),
		Product: m.product,
	}
	c, err := zkhelper.ConnectToZk(m.zkhost, 30)
	if err != nil {
		return errors.Trace(err)
	}
	defer c.Close()

	epic, err := models.GetEpic(c, m.product)
	if err != nil {
		return errors.Trace(err)
	}
	if epic == m.meta.Epic {
		/* no update */
		glog.V(8).Infof("equal epic %d", epic)
		return nil
	}

	for i := 0; i < models.DEFAULT_SLOT_NUM; i++ {
		slot, err := models.GetSlot(c, m.product, i)
		if err != nil {
			return errors.Trace(err)
		}
		data.Slots[fmt.Sprintf("%d", i)] = slot
		group_dic[slot.GroupId] = true
	}

	for idx, _ := range group_dic {
		group, err := models.GetGroup(c, m.product, idx)
		if err != nil {
			return errors.Trace(err)
		}
		data.Groups[fmt.Sprintf("%d", idx)] = group
	}

	data.Epic = epic
	m.meta = data
	glog.Infof("update meta done, epic: %v", epic)
	return nil
}

func (m *MetaServer) encode() ([]byte, error) {
	b, err := json.Marshal(m.meta)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Bytes(), nil
}

func (m *MetaServer) readEpic(r io.Reader) (uint64, error) {
	var buf [EPIC_SIZE]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, errors.Trace(err)
	}
	return utils.Uint64(buf[:]), nil
}

func (m *MetaServer) handle(c net.Conn) {
	defer c.Close()

	m.mu.RLock()
	defer m.mu.RUnlock()

	epic, err := m.readEpic(c)
	if err != nil {
		glog.Warningf("read epic failed - %s", err)
		return
	}
	glog.V(8).Infof("client: %s epic: %v", c.RemoteAddr().String(), epic)

	if epic > m.meta.Epic {
		glog.Warningf("possblie error client epic: %v current epic: %v", epic, m.meta.Epic)
		return
	}

	if epic == m.meta.Epic {
		glog.V(8).Infof("equal epic - %v", epic)
		return
	}

	b, err := m.encode()
	if err != nil {
		glog.Warningf("encode meta failed - %s", err)
		return
	}

	n, err := c.Write(b)
	if err != nil {
		glog.Warningf("write date failed - %s", err)
		return
	}
	glog.V(8).Infof("%v bytes sent to %s", n, c.RemoteAddr().String())
}

func (m *MetaServer) loop() {
	ticker := time.NewTicker(time.Second * 3)
	for {
		if err := m.collect(); err != nil {
			glog.Warningf("polling get meta failed - %s", err)
		}
		<-ticker.C
	}
}

func (m *MetaServer) Init() error {
	var err error
	m.ln, err = net.Listen("tcp", m.addr)
	if err != nil {
		return errors.Trace(err)
	}
	go m.loop()
	return nil
}

func (m *MetaServer) Run() {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			glog.Fatalf("accept failed - %s", err)
		}
		go m.handle(c)
	}
}
