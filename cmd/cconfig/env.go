// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"os"
	"strings"

	"github.com/c4pt0r/cfg"
	"github.com/docopt/docopt-go"

	"github.com/wandoulabs/zkhelper"

	"github.com/YongMan/codis/pkg/utils/errors"
	"github.com/YongMan/codis/pkg/utils/log"
)

type Env interface {
	ProductName() string
	Password() string
	DashboardAddr() string
	NewZkConn() (zkhelper.Conn, error)
	CollectorAddr() string
}

type CodisEnv struct {
	zkAddr        string
	passwd        string
	dashboardAddr string
	productName   string
	provider      string
	collectorAddr string
}

func LoadCodisEnv(cfg *cfg.Cfg) Env {
	if cfg == nil {
		log.Panicf("config is nil")
	}

	productName, err := cfg.ReadString("product", "test")
	if err != nil {
		log.PanicErrorf(err, "config: 'product' not found")
	}

	zkAddr, err := cfg.ReadString("zk", "localhost:2181")
	if err != nil {
		log.PanicErrorf(err, "config: 'zk' not found")
	}

	hostname, _ := os.Hostname()
	dashboardAddr, err := cfg.ReadString("dashboard_addr", hostname+":18087")
	if err != nil {
		log.PanicErrorf(err, "config: 'dashboard_addr' not found")
	}

	provider, err := cfg.ReadString("coordinator", "zookeeper")
	if err != nil {
		log.PanicErrorf(err, "config: 'coordinator' not found")
	}

	passwd, _ := cfg.ReadString("password", "")

	collectorAddr, _ := cfg.ReadString("collector_address", "")

	return &CodisEnv{
		zkAddr:        zkAddr,
		passwd:        passwd,
		dashboardAddr: dashboardAddr,
		productName:   productName,
		provider:      provider,
		collectorAddr: collectorAddr,
	}
}

func (e *CodisEnv) WriteMap() map[string]string {
	m := make(map[string]string)
	m["product"] = e.productName
	m["zk"] = e.zkAddr
	m["dashboard_addr"] = e.dashboardAddr
	m["coordinator"] = e.provider
	m["passwd"] = e.passwd
	m["collector_address"] = e.collectorAddr
	return m
}

func (e *CodisEnv) WriteCfg(file string) *cfg.Cfg {
	c := cfg.NewCfg(file)
	c.WriteString("product", e.productName)
	c.WriteString("zk", e.zkAddr)
	c.WriteString("dashboard_addr", e.dashboardAddr)
	c.WriteString("coordinator", e.provider)
	c.WriteString("password", e.passwd)
	c.WriteString("collector_address", e.collectorAddr)
	return c
}

func (e *CodisEnv) ProductName() string {
	return e.productName
}

func (e *CodisEnv) Password() string {
	return e.passwd
}

func (e *CodisEnv) DashboardAddr() string {
	return e.dashboardAddr
}

func (e *CodisEnv) CollectorAddr() string {
	return e.collectorAddr
}

func (e *CodisEnv) NewZkConn() (zkhelper.Conn, error) {
	switch e.provider {
	case "zookeeper":
		return zkhelper.ConnectToZk(e.zkAddr, 30)
	case "etcd":
		addr := strings.TrimSpace(e.zkAddr)
		if !strings.HasPrefix(addr, "http://") {
			addr = "http://" + addr
		}
		return zkhelper.NewEtcdConn(addr, 30)
	}
	return nil, errors.Errorf("need coordinator in config file, %s", e)
}

func cmdEnv(argv []string) error {
	usage := ` usage:
	codis-conifg env get <controller_addr> <file_path>
	`
	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	if args["get"].(bool) {
		addr := args["<controller_addr>"].(string)
		file := args["<file_path>"].(string)
		return runEnvGet(addr, file)
	}
	return nil
}

func runEnvGet(addr, file string) error {
	m := make(map[string]string)
	tmp := cfg.NewCfg(file)
	tmp.WriteString("dashboard_addr", addr)
	tmp.WriteString("product", "unknown")
	tmp.WriteString("zk", ":2181")
	tmp.WriteString("coordinator", "zk")
	tmp.WriteString("password", "")
	tmp.WriteString("collector_address", "")
	globalEnv = LoadCodisEnv(tmp)
	err := callApi(METHOD_GET, "/api/env", nil, &m)
	if err != nil {
		return err
	}

	c := cfg.NewCfg(file)
	for k, v := range m {
		c.WriteString(k, v)
	}

	if err := c.Save(); err != nil {
		return err
	}
	return nil
}
