// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/YongMan/codis/pkg/utils/errors"
)

func DialToTimeout(addr string, passwd string, readTimeout, writeTimeout time.Duration) (redis.Conn, error) {
	c, err := redis.DialTimeout("tcp", addr, time.Second*10, readTimeout, writeTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if passwd != "" {
		if _, err := c.Do("AUTH", passwd); err != nil {
			c.Close()
			return nil, errors.Trace(err)
		}
	}
	return c, nil
}

func DialTo(addr string, passwd string) (redis.Conn, error) {
	return DialToTimeout(addr, passwd, time.Hour, time.Hour)
}

func SlotsInfo(addr, passwd string, fromSlot, toSlot int) (map[int]int, error) {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	infos, err := redis.Values(c.Do("SLOTSINFO", fromSlot, toSlot-fromSlot+1))
	if err != nil {
		return nil, errors.Trace(err)
	}

	slots := make(map[int]int)
	if infos != nil {
		for i := 0; i < len(infos); i++ {
			info, err := redis.Values(infos[i], nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			var slotid, slotsize int
			if _, err := redis.Scan(info, &slotid, &slotsize); err != nil {
				return nil, errors.Trace(err)
			} else {
				slots[slotid] = slotsize
			}
		}
	}
	return slots, nil
}

var (
	ErrInvalidAddr       = errors.New("invalid addr")
	ErrStopMigrateByUser = errors.New("migration stopped by user")
)

func SlotsMgrtTagSlotWithConn(c redis.Conn, slotId int, toAddr string) (int, int, error) {
	addrParts := strings.Split(toAddr, ":")
	if len(addrParts) != 2 {
		return -1, -1, errors.Trace(ErrInvalidAddr)
	}
	reply, err := redis.String(c.Do("migrate_slot", slotId, addrParts[0], addrParts[1], 30000, 20000))
	if err != nil {
		return -1, -1, errors.Trace(err)
	}

	if strings.ToUpper(reply) == "DONE" {
		return 1, 0, nil
	} else {
		return 1, 1, nil
	}
}

func SlotsMgrtTagSlot(fromAddr string, passwd string, slotId int, toAddr string) (int, int, error) {
	conn, err := DialTo(fromAddr, passwd)
	if err != nil {
		return -1, -1, err
	}
	return SlotsMgrtTagSlotWithConn(conn, slotId, toAddr)
}

func SlotPreMigratingWithConn(c redis.Conn, slotId int, fromAddr string) error {
	addrParts := strings.Split(fromAddr, ":")
	if len(addrParts) != 2 {
		return errors.Trace(ErrInvalidAddr)
	}
	reply, err := redis.String(c.Do("slot_premigrating", slotId))
	if err != nil {
		return errors.Trace(err)
	}
	if strings.ToUpper(reply) != "OK" {
		return errors.Trace(fmt.Errorf("set slot: %d migrating failed", slotId))
	} else {
		return nil
	}
}

func SlotPreMigrating(addr string, passwd string, slotId int, fromAddr string) error {
	conn, err := DialTo(addr, passwd)
	if err != nil {
		return err
	}
	return SlotPreMigratingWithConn(conn, slotId, fromAddr)
}

func SlotPostMigratingWithConn(c redis.Conn, slotId int, formAddr string) error {
	addrParts := strings.Split(formAddr, ":")
	if len(addrParts) != 2 {
		return errors.Trace(ErrInvalidAddr)
	}
	reply, err := redis.String(c.Do("slot_postmigrating", slotId))
	if err != nil {
		return errors.Trace(err)
	}
	if strings.ToUpper(reply) != "OK" {
		return errors.Trace(fmt.Errorf("unset slot: %d migrating failed", slotId))
	} else {
		return nil
	}
}

func SlotPostMigrating(addr string, passwd string, slotId int, formAddr string) error {
	conn, err := DialTo(addr, passwd)
	if err != nil {
		return err
	}
	return SlotPostMigratingWithConn(conn, slotId, formAddr)
}

func SlotPreImportingWithConn(c redis.Conn, slotId int, toAddr string) error {
	addrParts := strings.Split(toAddr, ":")
	if len(addrParts) != 2 {
		return errors.Trace(ErrInvalidAddr)
	}
	reply, err := redis.String(c.Do("slot_preimporting", slotId))
	if err != nil {
		return errors.Trace(err)
	}
	if strings.ToUpper(reply) != "OK" {
		return errors.Trace(fmt.Errorf("set slot: %d importing failed", slotId))
	} else {
		return nil
	}
}

func SlotPreImporting(addr string, passwd string, slotId int, toAddr string) error {
	conn, err := DialTo(addr, passwd)
	if err != nil {
		return err
	}
	return SlotPreImportingWithConn(conn, slotId, toAddr)
}

func SlotPostImportingWithConn(c redis.Conn, slotId int, toAddr string) error {
	addrParts := strings.Split(toAddr, ":")
	if len(addrParts) != 2 {
		return errors.Trace(ErrInvalidAddr)
	}
	reply, err := redis.String(c.Do("slot_postimporting", slotId))
	if err != nil {
		return errors.Trace(err)
	}
	if strings.ToUpper(reply) != "OK" {
		return errors.Trace(fmt.Errorf("unset slot: %d importing failed", slotId))
	} else {
		return nil
	}
}

func SlotPostImporting(addr string, passwd string, slotId int, toAddr string) error {
	conn, err := DialTo(addr, passwd)
	if err != nil {
		return err
	}
	return SlotPostImportingWithConn(conn, slotId, toAddr)
}

func GetRedisStat(addr, passwd string) (map[string]string, error) {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	ret, err := redis.Strings(c.Do("INFO"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := make(map[string]string)
    for _, retline := range ret {
        lines := strings.Split(retline, "\n")
        for _, line := range lines {
            kv := strings.SplitN(line, ":", 2)
            if len(kv) == 2 {
                k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
                m[k] = v
            }
        }
    }

	/*reply, err := redis.Strings(c.Do("config", "get", "maxmemory"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	// we got result
	if len(reply) == 2 {
		if reply[1] != "0" {
			m["maxmemory"] = reply[1]
		} else {
			m["maxmemory"] = "∞"
		}
	}*/
	m["maxmemory"] = "0"
	return m, nil
}

func GetRedisConfig(addr, passwd string, configName string) (string, error) {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return "", err
	}
	defer c.Close()

	/*ret, err := redis.Strings(c.Do("config", "get", configName))
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(ret) == 2 {
		return ret[1], nil
	}*/
	return "", nil
}

func ConfigRewriteWithConn(c redis.Conn, addr string) error {
	if reply, err := redis.String(c.Do("config", "rewrite")); err != nil {
		return fmt.Errorf("%s fail to rewrite configuration - %s", err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s fail to rewrite configuration - reply not OK")
	}

	return nil
}

func SlaveOfWithConn(c redis.Conn, targetAddr, masterAddr string, last_seq int64) error {
	masterHost, masterPort, err := net.SplitHostPort(masterAddr)
	if err != nil {
		return fmt.Errorf("%s fail to change master to %s - %s", targetAddr, masterAddr, err)
	}

	/**
	 * stop slave
	 * there is no need to check stop_slave as change_master_to will return error if failed
	**/
	redis.Bool(c.Do("stop_slave"))

	/* change master and seq */
	if reply, err := redis.String(c.Do("change_master_to", masterHost, masterPort, last_seq, "")); err != nil {
		return fmt.Errorf("%s fail to change master to %s - %s", targetAddr, masterAddr, err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s fail to change master to %s - reply not OK", targetAddr, masterAddr)
	}

	if err := ConfigRewriteWithConn(c, targetAddr); err != nil {
		return err
	}

	/*
	 * start slave
	 * it is necessary to check if slave thread is on
	 */
	if reply, err := redis.Bool(c.Do("start_slave")); err != nil {
		return fmt.Errorf("%s fail to start slave to %s - %s", targetAddr, masterAddr, err)
	} else if !reply {
		return fmt.Errorf("%s fail to start slave to %s", targetAddr, masterAddr)
	}

	return nil
}

func SlaveOf(slave, passwd string, master string, last_seq int64) error {
	if master == slave {
		return errors.Errorf("can not slave of itself")
	}

	c, err := DialTo(slave, passwd)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := SlaveOfWithConn(c, slave, master, last_seq); err != nil {
		return err
	}

	return nil
}

func SlaveNoOneWithConn(c redis.Conn, addr string, last_seq int64) error {
	if last_seq <= 0 {
		/* full sync, do not check */
		redis.Bool(c.Do("stop_slave"))
	} else {
		/* psync, checking needed */
		if reply, err := redis.Bool(c.Do("stop_slave", last_seq)); err != nil {
			return fmt.Errorf("%s stop slave at seq %v failed - %s", addr, last_seq, err)
		} else if !reply {
			return fmt.Errorf("%s stop slave at seq %v failed", addr, last_seq)
		}
	}

	if reply, err := redis.String(c.Do("change_master_to", "", 0, 0, "")); err != nil {
		return fmt.Errorf("%s change master to no one failed - %s", addr, err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s change master to no one failed")
	}

	if err := ConfigRewriteWithConn(c, addr); err != nil {
		return err
	}

	return nil
}

func SlaveNoOne(addr, passwd string, last_seq int64) error {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return errors.Trace(err)
	}
	defer c.Close()

	if err := SlaveNoOneWithConn(c, addr, last_seq); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func ClientPause(addr, passwd string, timeout_ms int64) error {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return err
	}
	defer c.Close()

	if reply, err := redis.String(c.Do("client_pause", timeout_ms)); err != nil {
		return errors.Trace(err)
	} else if strings.ToUpper(reply) != "OK" {
		return errors.Trace(fmt.Errorf("fail to pause client"))
	}

	return nil
}

func DisableWrite(addr, passwd string) error {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return fmt.Errorf("%s fail to lock db with read lock - %s", addr, err)
	}
	defer c.Close()

	if reply, err := redis.String(c.Do("lock_db_with_read_lock")); err != nil {
		return fmt.Errorf("%s fail to lock db with read lock - %s", addr, err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s fail to lock db with read lock")
	}

	return nil
}

func EnableWrite(addr, passwd string) error {
	c, err := DialTo(addr, passwd)
	if err != nil {
		return fmt.Errorf("%s fail to unlockdb - %s", addr, err)
	}

	if reply, err := redis.String(c.Do("unlock_db")); err != nil {
		return fmt.Errorf("%s fail to unlock db - %s", addr, err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s fail to unlock db")
	}

	return nil
}

func SyncStatus(addr, passwd string) (map[string]string, error) {
	status := make(map[string]string)
	c, err := DialTo(addr, passwd)
	if err != nil {
		return status, fmt.Errorf("%s fail to get sync status - %s", addr, err)
	}
	defer c.Close()

	reply, err := redis.Values(c.Do("sync_status"))
	if err != nil {
		return status, fmt.Errorf("%s fail to get sync status - %s", addr, err)
	}

	if reply != nil && len(reply) > 3 {
		status, err = redis.StringMap(reply[1:], nil)
		if err != nil {
			return status, err
		}
		return status, nil
	}

	return status, nil
}

func BinlogSeq(addr, passwd string) (int64, error) {
	status, err := SyncStatus(addr, passwd)
	if err != nil {
		return 0, errors.Trace(err)
	}

	str, ok := status["binlog-seq"]
	if !ok {
		return 0, errors.Trace(errors.New("can't get binlog-seq"))
	}

	if v, err := strconv.ParseInt(str, 10, 64); err != nil {
		return 0, err
	} else {
		return v, nil
	}
}

func LastSeq(addr, passwd string) (int64, error) {
	status, err := SyncStatus(addr, passwd)
	if err != nil {
		return 0, errors.Trace(err)
	}

	str, ok := status["last-seq"]
	if !ok {
		return 0, errors.Trace(errors.New("can't get last-seq"))
	}

	if v, err := strconv.ParseInt(str, 10, 64); err != nil {
		return 0, err
	} else {
		return v, nil
	}
}

func SetSlot(addr string, slot int) error {
	c, err := DialTo(addr, "")
	if err != nil {
		return fmt.Errorf("%s failed to set slot %d - %s", addr, slot, err)
	}

	if reply, err := redis.String(c.Do("set_slot", slot)); err != nil {
		return fmt.Errorf("%d failed to set slot %d - %s", addr, slot, err)
	} else if strings.ToUpper(reply) != "OK" {
		return fmt.Errorf("%s failed to set slot %d - %s", addr, slot, err)
	}

	return nil
}
