//
// ha_monitor.go
// Copyright (C) 2016 yanming02 <yanming02@baidu.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"fmt"
	"time"

	"github.com/YongMan/codis/pkg/models"
	"github.com/YongMan/codis/pkg/utils/log"
	"github.com/garyburd/redigo/redis"
)

var IdcMap map[string]string
var filterMap map[string]int

const FILTER_COUNT = 5

func init() {
	IdcMap = map[string]string{
		"jx":    "bj",
		"tc":    "bj",
		"nj":    "nj",
		"nj03":  "nj",
		"nj01":  "nj",
		"hz":    "hz",
		"gz":    "gz",
		"gzhxy": "gz",
		"gzns":  "gz",
		"sh":    "sh",
		"nmg":   "nmg",
		"yq":    "yq",
	}
	filterMap = make(map[string]int)
}

func checkFilter(server string, filterCnt int) (int, bool) {
	if filterCnt == 0 {
		return 0, true
	}

	if _, ok := filterMap[server]; ok {
		filterMap[server]++
		if filterMap[server] > filterCnt {
			return filterMap[server], true
		}
		return filterMap[server], false
	} else {
		filterMap[server] = 1
		return 1, false
	}
}

func resetFilter(server string) {
	filterMap[server] = 0
}

func isAlive(ctx interface{}, ctxChan chan<- interface{}) {
	inner := func(addr string) bool {
		conn, err := redis.DialTimeout("tcp", addr, 5*time.Second, 5*time.Second, 5*time.Second)
		if err != nil {
			return false
		}
		defer conn.Close()
		resp, err := redis.String(conn.Do("PING"))
		if err != nil || resp != "PONG" {
			return false
		}
		return true
	}

	for i := 0; i < 3; i++ {
		if inner(ctx.(*models.Server).Addr) == true {
			ctxChan <- nil
			return
		}
	}

	ctxChan <- ctx
}

func GetServerGroups() ([]models.ServerGroup, error) {
	var groups []models.ServerGroup
	err := callApi(METHOD_GET, "/api/server_groups", nil, &groups)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

func verifyAndUpServer(ctx interface{}) {
	ctxChan := make(chan interface{}, 1000)

	go isAlive(ctx, ctxChan)

	s := <-ctxChan
	if s == nil { //alive
		log.Infof("Server become alive, add %s to cluster", ctx.(*models.Server).Addr)
		handleAddServer(ctx.(*models.Server))
	}

}

func handleCrashedServer(s *models.Server, groups []models.ServerGroup) error {

	inner := func(s *models.Server, groups []models.ServerGroup) *models.Server {
		// return a server in the same idc with server_addr
		for _, group := range groups {
			if group.Id == s.GroupId {
				for _, server := range group.Servers {
					if server.Addr == s.Addr {
						continue
					}
					if IdcMap[server.Tag] == IdcMap[s.Tag] {
						return server
					}
				}
				return nil
			}
		}
		return nil
	}

	switch s.Type {
	case models.SERVER_TYPE_MASTER:
		times, valid := checkFilter(s.Addr, FILTER_COUNT)
		log.Infof("Found dead master server: %s, filter: %d/%d", s.Addr, times, FILTER_COUNT)
		if !valid {
			return nil
		}
		// reset filter cnt
		resetFilter(s.Addr)
		// call failover api
		var v interface{}
		// find available server tag in this group
		as := inner(s, groups)
		if as == nil {
			return fmt.Errorf("No available server in the same region with server:%s tag:%s", s.Addr, s.Tag)
		}
		err := callApi(METHOD_POST, fmt.Sprintf("/api/server_group/%d/failover/%s/%s", s.GroupId, "SYNC", as.Tag), nil, &v)
		if err != nil {
			log.Errorf(err.Error())
			return err
		}
		log.Infof(jsonify(v))
	case models.SERVER_TYPE_SLAVE:
		// call server control api
		times, valid := checkFilter(s.Addr, FILTER_COUNT)
		log.Infof("Found dead slave server: %s, filter: %d", s.Addr, times)
		if !valid {
			return nil
		}
		// reset filter cnt
		resetFilter(s.Addr)
		var v interface{}
		s.Type = models.SERVER_TYPE_DISABLE
		err := callApi(METHOD_PUT, fmt.Sprintf("/api/server_group/%d/serverModeCtrl", s.GroupId), s, &v)
		if err != nil {
			log.Errorf(err.Error())
			return nil
		}
		log.Infof(jsonify(v))
	case models.SERVER_TYPE_OFFLINE:
		//no need to handle it
		log.Infof("Found offline server: %s, need to fix manually", s.Addr)
	default:
	}

	return nil
}

func handleAddServer(s *models.Server) error {
	s.Type = models.SERVER_TYPE_SLAVE
	err := callApi(METHOD_PUT, fmt.Sprintf("/api/server_group/%d/addServer", s.GroupId), s, nil)
	if err != nil {
		return err
	}
	return nil
}

func CheckAliveAndPromote(groups []models.ServerGroup) ([]*models.Server, error) {
	ctxChan := make(chan interface{}, 1000)
	var serverCnt int
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			serverCnt++
			go isAlive(s, ctxChan)
		}
	}

	var crashedServer []*models.Server
	for i := 0; i < serverCnt; i++ {
		s := <-ctxChan
		if s == nil { //alive
			continue
		}
		crashedServer = append(crashedServer, s.(*models.Server))
	}

	// check constraint
	// 挂掉节点超过1/3或者挂掉节点超过10个，需要人工介入
	if len(crashedServer)*3 > serverCnt {
		return crashedServer, fmt.Errorf("more than 1/3 nodes crashed")
	}
	if len(crashedServer) > 10 {
		return crashedServer, fmt.Errorf("more than 10 nodes crashed")
	}

	// do handle crashedServer
	for _, s := range crashedServer {
		err := handleCrashedServer(s, groups)
		if err != nil {
			return nil, err
		}
	}

	return crashedServer, nil
}

func CheckOfflineAndPromoteSlave(groups []models.ServerGroup) ([]models.Server, error) {
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			if s.Type == models.SERVER_TYPE_OFFLINE {
				verifyAndUpServer(s)
			}
		}
	}
	return nil, nil
}

func RunHaMonitor() {
	time.Sleep(5 * time.Second)

	for {
		log.Infof("HA Monitor update groups")
		groups, err := GetServerGroups()
		if err != nil {
			log.Errorf(err.Error())
			time.Sleep(10 * time.Second)
			continue
		}
		log.Infof("HA Monitor check db servers")

		_, err = CheckAliveAndPromote(groups)
		if err != nil {
			log.Errorf(err.Error())
		}

		//CheckOfflineAndPromoteSlave(groups)
		time.Sleep(10 * time.Second)
	}
}
