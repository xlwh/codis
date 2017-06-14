package proxy

import (
	"encoding/json"
	"strings"

	"github.com/ksarch-saas/codis/pkg/utils/log"
	"github.com/c4pt0r/cfg"
)

type Config struct {
	proxyId     string
	productName string
	metaAddrs   string
	proxyAddr   string
	tag         string
	backup      map[string][]string

	maxTimeout      int
	maxBufSize      int
	maxPipeline     int
	pollingInterval int
	keepAlivePeriod int
	slowThreshold   int
}

func LoadConf(configFile string) (*Config, error) {
	c := cfg.NewCfg(configFile)
	if err := c.Load(); err != nil {
		log.PanicErrorf(err, "load config '%s' failed", configFile)
	}

	conf := &Config{}
	conf.productName, _ = c.ReadString("product", "test")
	if len(conf.productName) == 0 {
		log.Panicf("invalid config: product entry is missing in %s", configFile)
	}
	conf.metaAddrs, _ = c.ReadString("meta", "")
	if len(conf.metaAddrs) == 0 {
		log.Panicf("invalid config: meta entry is missing in %s", configFile)
	}
	conf.metaAddrs = strings.TrimSpace(conf.metaAddrs)
	conf.proxyId, _ = c.ReadString("proxy_id", "")
	if len(conf.proxyId) == 0 {
		log.Panicf("invalid config: proxy_id entry is missing in %s", configFile)
	}

	conf.tag, _ = c.ReadString("tag", "jx")
	backup, _ := c.ReadString("backup", "{\"jx\":[\"tc\"],\"tc\":[\"jx\"],\"nj\":[\"nj03\"],\"nj03\":[\"nj\"]}")
	if err := json.Unmarshal([]byte(backup), &conf.backup); err != nil {
		log.Panicf("parse backup %s failed - %s", string(backup), err)
	}

	loadConfInt := func(entry string, defval int) int {
		v, _ := c.ReadInt(entry, defval)
		if v < 0 {
			log.Panicf("invalid config: read %s = %d", entry, v)
		}
		return v
	}

	conf.maxTimeout = loadConfInt("session_max_timeout", 1800)
	conf.maxBufSize = loadConfInt("session_max_bufsize", 131072)
	conf.maxPipeline = loadConfInt("session_max_pipeline", 1024)
	conf.pollingInterval = loadConfInt("topo_loop_interval", 1)
	conf.keepAlivePeriod = loadConfInt("session_keep_alive_period", 60)
	conf.slowThreshold = loadConfInt("slow_threshold", 100)
	return conf, nil
}
