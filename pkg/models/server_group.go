// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ksarch-saas/codis/pkg/utils"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
	"github.com/ksarch-saas/codis/pkg/utils/log"
	"github.com/wandoulabs/zkhelper"
)

const (
	SERVER_TYPE_MASTER  string = "master"
	SERVER_TYPE_SLAVE   string = "slave"
	SERVER_TYPE_OFFLINE string = "offline"
	SERVER_TYPE_DISABLE string = "disable"
)

// redis server instance
type Server struct {
	Type    string `json:"type"`
	GroupId int    `json:"group_id"`
	Addr    string `json:"addr"`
	Tag     string `json:"tag"`
}

const (
	REPL_SYNC = iota
	REPL_PSYNC
)

type ReplInfo struct {
	ReplMode  int
	TargetSeq int64
	MasterSeq int64
}

// redis server group
type ServerGroup struct {
	Id               int       `json:"id"`
	ProductName      string    `json:"product_name"`
	Servers          []*Server `json:"servers"`
	ReplInfo         ReplInfo  `json:"-"`
	CachedMasterAddr string    `json:"-"`
}

func (self *Server) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b)
}

func (self *ServerGroup) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b) + "\n"
}

func GetServer(zkConn zkhelper.Conn, zkPath string) (*Server, error) {
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	srv := Server{}
	if err := json.Unmarshal(data, &srv); err != nil {
		return nil, errors.Trace(err)
	}
	return &srv, nil
}

func NewServer(serverType string, addr string, tag string) *Server {
	return &Server{
		Type:    serverType,
		GroupId: INVALID_ID,
		Addr:    addr,
		Tag:     tag,
	}
}

func NewServerGroup(productName string, id int) *ServerGroup {
	return &ServerGroup{
		Id:          id,
		ProductName: productName,
	}
}

func GroupExists(zkConn zkhelper.Conn, productName string, groupId int) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", productName, groupId)
	exists, _, err := zkConn.Exists(zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return exists, nil
}

func GetGroup(zkConn zkhelper.Conn, productName string, groupId int) (*ServerGroup, error) {
	exists, err := GroupExists(zkConn, productName, groupId)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, errors.Errorf("group %d is not found", groupId)
	}

	group := &ServerGroup{
		ProductName: productName,
		Id:          groupId,
	}

	group.Servers, err = group.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return group, nil
}

func ServerGroups(zkConn zkhelper.Conn, productName string) ([]*ServerGroup, error) {
	var ret []*ServerGroup
	root := fmt.Sprintf("/zk/codis/db_%s/servers", productName)
	groups, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Buggy :X
	//zkhelper.ChildrenRecursive(*zkConn, root)

	for _, group := range groups {
		// parse group_1 => 1
		groupId, err := strconv.Atoi(strings.Split(group, "_")[1])
		if err != nil {
			return nil, errors.Trace(err)
		}
		g, err := GetGroup(zkConn, productName, groupId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, g)
	}
	return ret, nil
}

func (self *ServerGroup) Master(zkConn zkhelper.Conn) (*Server, error) {
	servers, err := self.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == SERVER_TYPE_MASTER {
			return s, nil
		}
	}
	return nil, nil
}

func (self *ServerGroup) Remove(zkConn zkhelper.Conn) error {
	// check if this group is not used by any slot
	slots, err := Slots(zkConn, self.ProductName)
	if err != nil {
		return errors.Trace(err)
	}

	for _, slot := range slots {
		if slot.GroupId == self.Id {
			return errors.Errorf("group %d is using by slot %d", slot.GroupId, slot.Id)
		}
		if (slot.State.Status == SLOT_STATUS_MIGRATE || slot.State.Status == SLOT_STATUS_PRE_MIGRATE) && slot.State.MigrateStatus.From == self.Id {
			return errors.Errorf("slot %d has residual data remain in group %d", slot.Id, self.Id)
		}
	}

	// do delete
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	err = zkhelper.DeleteRecursive(zkConn, zkPath, -1)

	/* increase epci to trigger collector update */
	err = IncEpic(zkConn, self.ProductName)
	return errors.Trace(err)
}

func (self *ServerGroup) getServerPath(addr string) string {
	return fmt.Sprintf("/zk/codis/db_%s/servers/group_%d/%s", self.ProductName, self.Id, addr)
}

func (self *ServerGroup) delServerMeta(zkConn zkhelper.Conn, addr string) error {
	zkPath := self.getServerPath(addr)
	if err := zkConn.Delete(zkPath, -1); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (self *ServerGroup) getServerMeta(zkConn zkhelper.Conn, addr string) (*Server, error) {
	zkPath := self.getServerPath(addr)
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var s Server
	err = json.Unmarshal(data, &s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &s, nil
}

func (self *ServerGroup) updateServerMeta(zkConn zkhelper.Conn, s *Server) error {
	zkPath := self.getServerPath(s.Addr)
	val, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := zkhelper.CreateOrUpdate(zkConn, zkPath, string(val), 0, zkhelper.DefaultDirACLs(), true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (self *ServerGroup) DisableServer(zkConn zkhelper.Conn, addr string) error {
	s, err := self.getServerMeta(zkConn, addr)
	if err != nil {
		return errors.Trace(err)
	}
	/* master cannot be disable */
	if s.Type == SERVER_TYPE_MASTER {
		return errors.Errorf("cannot disable master")
	}
	if s.Type == SERVER_TYPE_OFFLINE {
		return errors.Errorf("server is offline")
	}
	/* return ASAP if disable */
	if s.Type != SERVER_TYPE_SLAVE {
		return nil
	}
	s.Type = SERVER_TYPE_DISABLE

	if err := self.updateServerMeta(zkConn, s); err != nil {
		return errors.Trace(err)
	}

	/* increase epci to trigger collector update */
	err = IncEpic(zkConn, self.ProductName)

	return errors.Trace(err)
}

func (self *ServerGroup) EnableServer(zkConn zkhelper.Conn, addr string) error {
	s, err := self.getServerMeta(zkConn, addr)
	if err != nil {
		return errors.Trace(err)
	}
	/* server offline cannot enable */
	if s.Type == SERVER_TYPE_OFFLINE {
		return errors.Errorf("server is offline")
	}
	if s.Type == SERVER_TYPE_MASTER {
		return errors.Errorf("server is master")
	}
	/* return ASAP if enable */
	if s.Type != SERVER_TYPE_DISABLE {
		return nil
	}
	s.Type = SERVER_TYPE_SLAVE

	if err := self.updateServerMeta(zkConn, s); err != nil {
		return errors.Trace(err)
	}

	/* increase epci to trigger collector update */
	err = IncEpic(zkConn, self.ProductName)

	return errors.Trace(err)
}

func (self *ServerGroup) RemoveServer(zkConn zkhelper.Conn, addr string) error {
	s, err := self.getServerMeta(zkConn, addr)
	if err != nil {
		return errors.Trace(err)
	}

	if s.Type == SERVER_TYPE_MASTER {
		return errors.Errorf("cannot remove master, use promote first")
	}

	if err := self.delServerMeta(zkConn, addr); err != nil {
		return errors.Trace(err)
	}

	// update server list
	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == s.Addr {
			self.Servers = append(self.Servers[:i], self.Servers[i+1:]...)
			break
		}
	}

	/* increase epci to trigger collector update */
	err = IncEpic(zkConn, self.ProductName)

	return errors.Trace(err)
}

func (self *ServerGroup) PSYNCSelectCandidate(tag string) (*Server, error) {
	for _, s := range self.Servers {
		/* always return the first target */
		if s.Tag == tag && s.Type == SERVER_TYPE_SLAVE {
			log.Infof("PSYNC select %s tag:%s", s.Addr, tag)
			return s, nil
		}
	}
	return nil, fmt.Errorf("PSYNC select candidate failed tag: %s", tag)
}

func (self *ServerGroup) SYNCSelectCandidate(passwd string, tag string) (*Server, error) {
	/* find the most updated slave */
	id := -1
	maxSeq := int64(-1)
	for i, s := range self.Servers {
		if s.Tag == tag && s.Type == SERVER_TYPE_SLAVE {
			if seq, err := utils.LastSeq(s.Addr, passwd); err != nil {
				return nil, fmt.Errorf("SYNC select candidate failed - get %s seq failed - %s", s.Addr, err)
			} else {
				if seq > maxSeq {
					maxSeq = seq
					id = i
				}
			}
		}
	}
	if id == -1 {
		return nil, fmt.Errorf("SYNC select candidate failed tag: %s", tag)
	}
	log.Infof("SYNC select %s tag: %s seq: %v", self.Servers[id].Addr, tag, maxSeq)
	return self.Servers[id], nil
}

func (self *ServerGroup) PSYNCInit(passwd string, master, target *Server) error {
	log.Info("PSYNC init")
	log.Infof("disable write - %s", target.Addr)
	/* disable write of target and current master */
	if err := utils.DisableWrite(target.Addr, passwd); err != nil {
		return errors.Trace(err)
	}

	log.Infof("disable write - %s", master.Addr)
	if err := utils.DisableWrite(master.Addr, passwd); err != nil {
		return errors.Trace(err)
	}

	self.ReplInfo.ReplMode = REPL_PSYNC

	/* get master last seq */
	if seq, err := utils.BinlogSeq(master.Addr, passwd); err != nil {
		return errors.Trace(err)
	} else {
		self.ReplInfo.MasterSeq = seq
		log.Infof("master seq: %v", seq)
	}

	/* block and promote target as master at the specified seq */
	log.Infof("%s slave of no one at seq: %v", target.Addr, self.ReplInfo.MasterSeq)
	if err := utils.SlaveNoOne(target.Addr, passwd, self.ReplInfo.MasterSeq); err != nil {
		return errors.Trace(err)
	}

	/* get target last seq */
	if seq, err := utils.BinlogSeq(target.Addr, passwd); err != nil {
		return errors.Trace(err)
	} else {
		self.ReplInfo.TargetSeq = seq
		log.Infof("target binlog seq: %v", seq)
	}

	log.Infof("%s slave of %s at seq: %v", master.Addr, target.Addr, self.ReplInfo.TargetSeq)
	if err := utils.SlaveOf(master.Addr, passwd, target.Addr, self.ReplInfo.TargetSeq); err != nil {
		return errors.Trace(err)
	}

	self.CachedMasterAddr = master.Addr
	log.Infof("cached master - %s", master.Addr)

	return nil
}

func (self *ServerGroup) SYNCInit(passwd string, target *Server) error {
	log.Info("SYNC init")
	/* try promoting a slave then psync with the new master. */
	seq, err := utils.LastSeq(target.Addr, passwd)
	if err != nil {
		return errors.Trace(err)
	}
	self.ReplInfo.MasterSeq = seq
	self.CachedMasterAddr = ""
	log.Infof("%s last seq:%v", target.Addr, seq)

	/* get the binlog seq of the target */
	seq, err = utils.BinlogSeq(target.Addr, passwd)
	if err != nil {
		return errors.Trace(err)
	}
	self.ReplInfo.TargetSeq = seq
	log.Infof("%s binlog seq:%v", target.Addr, seq)

	/* disconnect with origin master */
	if err := utils.SlaveNoOne(target.Addr, passwd, 0); err != nil {
		return errors.Trace(err)
	}

	/* set replication flag */
	self.ReplInfo.ReplMode = REPL_SYNC

	return nil
}

func (self *ServerGroup) Failover(conn zkhelper.Conn, passwd string, psync bool, tag string) (string, error) {
	log.Infof("group %d failover psync:%v tag:%s", self.Id, psync, tag)
	/* find master */
	master, err := self.Master(conn)
	if err != nil {
		return "", errors.Trace(err)
	}

	if master != nil {
		if !psync {
			/* disable current master while SYNC */
			master.Type = SERVER_TYPE_OFFLINE
			if err := self.AddServer(conn, master, passwd); err != nil {
				return "", errors.Trace(err)
			}
		}
	} else if psync {
		return "", errors.Trace(fmt.Errorf("PSYNC failed - master not exists"))
	}

	var target *Server
	if psync {
		/* get candidate */
		s, err := self.PSYNCSelectCandidate(tag)
		if err != nil {
			return "", errors.Trace(err)
		}
		target = s

		if err := self.PSYNCInit(passwd, master, target); err != nil {
			return "", errors.Trace(err)
		}

		/* flag origin master as slave */
		master.Type = SERVER_TYPE_SLAVE
		if err := self.updateServerMeta(conn, master); err != nil {
			return "", errors.Trace(err)
		}
	} else {
		/* try to disable write in case of master still alive. */
		if master != nil {
			log.Infof("try to disable write - %s", master.Addr)
			utils.DisableWrite(master.Addr, passwd)
		}

		/* get candidate */
		s, err := self.SYNCSelectCandidate(passwd, tag)
		if err != nil {
			return "", errors.Trace(err)
		}
		target = s

		if err := self.SYNCInit(passwd, target); err != nil {
			return "", errors.Trace(err)
		}
	}

	/* promote new server to master. */
	target.Type = SERVER_TYPE_MASTER
	err = self.AddServer(conn, target, passwd)
	return target.Addr, errors.Trace(err)
}

func (self *ServerGroup) Promote(conn zkhelper.Conn, addr, passwd string, psync bool) error {
	log.Infof("server promote: %s psync: %v", addr, psync)
	var target *Server
	exists := false
	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == addr {
			target = self.Servers[i]
			exists = true
			break
		}
	}

	if !exists {
		return errors.Errorf("no such addr %s", addr)
	}

	// set origin master offline
	master, err := self.Master(conn)
	if err != nil {
		return errors.Trace(err)
	}

	// old master may be nil
	if master != nil {
		/* there is no need to mark current master offline */
		if !psync {
			master.Type = SERVER_TYPE_OFFLINE
			err = self.AddServer(conn, master, passwd)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else if psync {
		return errors.Trace(fmt.Errorf("PSYNC promote failed - master not exists"))
	}

	if psync {
		if err := self.PSYNCInit(passwd, master, target); err != nil {
			return errors.Trace(err)
		}

		/* set origin master as slave */
		master.Type = SERVER_TYPE_SLAVE
		if err := self.updateServerMeta(conn, master); err != nil {
			return errors.Trace(err)
		}
	} else {
		/* try to disable write in case of master still alive. */
		if master != nil {
			log.Infof("disable write %s", master.Addr)
			utils.DisableWrite(master.Addr, passwd)
		}

		if err := self.SYNCInit(passwd, target); err != nil {
			return errors.Trace(err)
		}
	}

	// promote new server to master
	target.Type = SERVER_TYPE_MASTER
	err = self.AddServer(conn, target, passwd)
	return errors.Trace(err)
}

func (self *ServerGroup) Create(zkConn zkhelper.Conn) error {
	if self.Id < 0 {
		return errors.Errorf("invalid server group id %d", self.Id)
	}
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	_, err := zkhelper.CreateOrUpdate(zkConn, zkPath, "", 0, zkhelper.DefaultDirACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}
	/* increase epci to trigger collector update */
	err = IncEpic(zkConn, self.ProductName)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (self *ServerGroup) Exists(zkConn zkhelper.Conn) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	b, err := zkhelper.NodeExists(zkConn, zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return b, nil
}

var ErrNodeExists = errors.New("node already exists")

func (self *ServerGroup) AddServer(zkConn zkhelper.Conn, s *Server, passwd string) error {
	s.GroupId = self.Id

	servers, err := self.GetServers(zkConn)
	if err != nil {
		return errors.Trace(err)
	}
	var masterAddr string
	for _, server := range servers {
		if server.Type == SERVER_TYPE_MASTER {
			masterAddr = server.Addr
		}
	}

	// make sure there is only one master
	if s.Type == SERVER_TYPE_MASTER && len(masterAddr) > 0 {
		return errors.Trace(ErrNodeExists)
	}

	// if this group has no server. auto promote this server to master
	if len(servers) == 0 {
		s.Type = SERVER_TYPE_MASTER
	}

	if err := self.updateServerMeta(zkConn, s); err != nil {
		return errors.Trace(err)
	}

	// update servers
	servers, err = self.GetServers(zkConn)
	if err != nil {
		return errors.Trace(err)
	}
	self.Servers = servers

	if s.Type == SERVER_TYPE_MASTER {
		if self.ReplInfo.ReplMode == REPL_PSYNC {
			/* concurrent PSYNC */
			ch := make(chan string)
			count := 0
			for _, server := range servers {
				if server.Type == SERVER_TYPE_MASTER || server.Addr == self.CachedMasterAddr || server.Type == SERVER_TYPE_OFFLINE {
					continue
				}
				count++

				/**
				 * change slaves's master to the new one
				 * slave no one with seq > 0 will block for a while,
				 * so we process slave of new master concurrently
				**/
				go func(addr string) {
					/* stop slave at the master's last seq */
					if err := utils.SlaveNoOne(addr, passwd, self.ReplInfo.MasterSeq); err != nil {
						ch <- err.Error()
					}

					/* start slave at target's last seq */
					if err := utils.SlaveOf(addr, passwd, s.Addr, self.ReplInfo.TargetSeq); err != nil {
						ch <- err.Error()
					}
					ch <- ""
				}(server.Addr)
			}

			var buf bytes.Buffer
			has_err := false
			for i := 0; i < count; i++ {
				str := <-ch
				if len(str) > 0 {
					has_err = true
					buf.WriteString(str)
					buf.WriteString("\n")
				}
			}

			if has_err {
				return fmt.Errorf("%s", buf.String())
			}
		} else {
			/* SYNC */
			dict := make(map[string]int64)
			/**
			 * caculate and record new seq for all slaves
			 * check full sync
			 **/
			for _, server := range servers {
				if server.Type == SERVER_TYPE_MASTER || server.Type == SERVER_TYPE_OFFLINE {
					continue
				}

				/* change slaves' master to the new one, full SYNC foribbiden */
				lastSeq, err := utils.LastSeq(server.Addr, passwd)
				if err != nil {
					return errors.Trace(err)
				}
				offset := self.ReplInfo.MasterSeq - lastSeq
				log.Infof("%s last seq: %v offset: %v", server.Addr, lastSeq, offset)
				if offset < 0 {
					return errors.Trace(fmt.Errorf("detect full SYNC for slave - %s seq: %v master - %s seq: %v",
						server.Addr, lastSeq, s.Addr, self.ReplInfo.MasterSeq))
				}
				dict[server.Addr] = self.ReplInfo.TargetSeq - offset
			}

			/**
			 * don't stop process when come up to an error for prossible io timeout
			 * getogether error message and return at last
			 **/
			var buf bytes.Buffer
			has_err := false
			for k, v := range dict {
				log.Infof("%s slave of %s at %v", k, s.Addr, v)
				err := utils.SlaveOf(k, passwd, s.Addr, v)
				if err != nil {
					has_err = true
					emsg := fmt.Sprintf("%s slave of %s at %v failed\n", k, s.Addr, v)
					buf.WriteString(emsg)
				}
			}

			if has_err {
				return fmt.Errorf("%s", buf.String())
			}
		}

		/* enable write */
		log.Infof("enable write: %s", s.Addr)
		if err := utils.EnableWrite(s.Addr, passwd); err != nil {
			return errors.Trace(err)
		}

		/* increase epci to trigger collector update */
		err = IncEpic(zkConn, self.ProductName)
		if err != nil {
			return errors.Trace(err)
		}
	} else if (s.Type == SERVER_TYPE_SLAVE || s.Type == SERVER_TYPE_DISABLE) && len(masterAddr) > 0 {
		// send command slaveof to slave
		err := utils.SlaveOf(s.Addr, passwd, masterAddr, 0)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (self *ServerGroup) GetServers(zkConn zkhelper.Conn) ([]*Server, error) {
	var ret []*Server
	root := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	nodes, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, node := range nodes {
		nodePath := root + "/" + node
		s, err := GetServer(zkConn, nodePath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, s)
	}
	return ret, nil
}
