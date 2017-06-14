package proxy

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"time"

	"github.com/ksarch-saas/codis/pkg/models"
	"github.com/ksarch-saas/codis/pkg/utils"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
)

type Topo struct {
	metaServers []string
	meta        *models.Meta
	product     string
	interval    int
	notify      chan *models.Meta
	exit        chan int
}

func NewTopo(product string, addrs []string, interval int) *Topo {
	if interval < 1 {
		interval = 1
	}

	t := &Topo{
		metaServers: addrs,
		meta:        &models.Meta{Epic: 0},
		product:     product,
		interval:    interval,
		notify:      make(chan *models.Meta),
		exit:        make(chan int),
	}
	return t
}

func (t *Topo) Run() {
	go t.watch()
}

func (t *Topo) Close() {
	t.exit <- 0
}

func (t *Topo) Notify() chan *models.Meta {
	return t.notify
}

func (t *Topo) GetMeta() *models.Meta {
	return t.meta
}

func (t *Topo) qurey() ([]byte, error) {
	h := t.metaServers[rand.Int()%len(t.metaServers)]
	c, err := net.DialTimeout("tcp", h, time.Second)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.Close()

	epic := make([]byte, 8)
	utils.PutUint64(epic, t.meta.Epic)
	if _, err := c.Write(epic); err != nil {
		return nil, errors.Trace(err)
	}

	b, err := ioutil.ReadAll(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
}

func (t *Topo) update() error {
	b, err := t.qurey()
	if err != nil {
		return errors.Trace(err)
	}

	if b == nil || len(b) == 0 {
		log.Debug("no need to update")
		return nil
	}
	log.Infof("%d bytes receieved", len(b))

	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return errors.Trace(err)
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Trace(err)
	}

	m := &models.Meta{}
	if err := json.Unmarshal(data, m); err != nil {
		return errors.Trace(err)
	}

	/* check product */
	if m.Product != t.product {
		return fmt.Errorf("invailidate product: %s", m.Product)
	}

	t.meta = m
	t.notify <- m
	log.Infof("meta update done, epic - %v", t.meta.Epic)
	return nil
}

func (t *Topo) watch() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		interval := t.interval
		if err := t.update(); err != nil {
			interval = 2
			log.WarnErrorf(err, "get meta failed")
		}
		for i := 0; i < interval; i++ {
			select {
			case <-t.exit:
				return
			case <-ticker.C:
			}
		}
	}
}
