package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"gitlab.baidu.com/go/glog"

	"github.com/ksarch-saas/codis/pkg/meta"
)

var (
	address string
	product string
	zkhost  string
	version bool
)

func init() {
	flag.StringVar(&address, "a", ":8999", "server address")
	flag.StringVar(&zkhost, "z", ":2183", "zookeeper host")
	flag.StringVar(&product, "p", "ksarch", "product id for the cluster")
	flag.BoolVar(&version, "version", false, "show version")
}

func showVersion() {
	fmt.Println("ssdb meta server version - 0.0.0.2 author - yanyu - 2016-10-10")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()
	if version {
		showVersion()
		return
	}

	defer glog.Flush()

	m := meta.NewMetaServer(address, product, zkhost)
	if err := m.Init(); err != nil {
		glog.Fatalf("meta init failed - %s", err)
		return
	}

	m.Run()
}
