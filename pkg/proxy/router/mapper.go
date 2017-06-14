// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"bytes"
	//"hash/crc32"
	"strings"

	"gitlab.baidu.com/store/hashkit"

	"github.com/ksarch-saas/codis/pkg/proxy/redis"
	"github.com/ksarch-saas/codis/pkg/utils/errors"
)

var charmap [128]byte

func init() {
	for i := 0; i < len(charmap); i++ {
		c := byte(i)
		if c >= 'a' && c <= 'z' {
			c = c - 'a' + 'A'
		}
		charmap[i] = c
	}
}

var (
	blacklist = make(map[string]bool)
	OpWrite   = make(map[string]bool)
	OpRead    = make(map[string]bool)
)

func init() {
	for _, s := range []string{
		"KEYS", "MOVE", "OBJECT", "RENAME", "RENAMENX", "SCAN", "BITOP", "MSETNX", "MIGRATE", "RESTORE",
		"BLPOP", "BRPOP", "BRPOPLPUSH", "PSUBSCRIBE", "PUBLISH", "PUNSUBSCRIBE", "SUBSCRIBE", "RANDOMKEY",
		"UNSUBSCRIBE", "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH", "SCRIPT",
		"BGREWRITEAOF", "BGSAVE", "CLIENT", "CONFIG", "DBSIZE", "DEBUG", "FLUSHALL", "FLUSHDB",
		"LASTSAVE", "MONITOR", "SAVE", "SHUTDOWN", "SLAVEOF", "SLOWLOG", "SYNC", "TIME",
		"SLOTSINFO", "SLOTSDEL", "SLOTSMGRTSLOT", "SLOTSMGRTONE", "SLOTSMGRTTAGSLOT", "SLOTSMGRTTAGONE", "SLOTSCHECK",
	} {
		blacklist[s] = true
	}

	for _, s := range []string{
		"GET", "EXISTS", "TTL", "GETBIT", "STRLEN", "BITCOUNT", "SUBSTR", "GETRANGE", "HGET", "HEXISTS",
		"HMGET", "HLEN", "ZCARD", "ZSCORE", "ZRANK", "ZCOUNT", "MGET", "HGETALL", "HKEYS", "HVALS", "ZRANGE",
		"ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "LLEN", "LSZIE", "LINDEX", "LRANGE",
		"SISMEMBER", "SMEMBERS", "SCARD",
	} {
		OpRead[s] = true
	}

	for _, s := range []string{
		"GETSET", "SET", "SETNX", "INCR", "DECR", "EXPIRE", "EXPIREAT", "SETBIT", "HSET", "DEL", "MSET", "INCRBY",
		"DECRBY", "HMSET", "HDEL", "HMDEL", "HINCRBY", "ZREM", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "SETEX", "ZADD",
		"ZINCRBY", "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM", "SDEL", "SADD", "PEXPIRE", "PEXPIREAT",
	} {
		OpWrite[s] = true
	}
}

func isNotAllowed(opstr string) bool {
	return blacklist[opstr]
}

func opCheck(opstr string) (bool, bool) {
	r := OpRead[strings.ToUpper(opstr)]
	w := OpWrite[strings.ToUpper(opstr)]
	return r || w, w
}

var (
	ErrBadRespType = errors.New("bad resp type for command")
	ErrBadOpStrLen = errors.New("bad command length, too short or too long")
)

func getOpStr(resp *redis.Resp) (string, error) {
	if !resp.IsArray() || len(resp.Array) == 0 {
		return "", ErrBadRespType
	}
	for _, r := range resp.Array {
		if r.IsBulkBytes() {
			continue
		}
		return "", ErrBadRespType
	}

	var upper [64]byte

	var op = resp.Array[0].Value
	if len(op) == 0 || len(op) > len(upper) {
		return "", ErrBadOpStrLen
	}
	for i := 0; i < len(op); i++ {
		c := uint8(op[i])
		if k := int(c); k < len(charmap) {
			upper[i] = charmap[k]
		} else {
			return strings.ToUpper(string(op)), nil
		}
	}
	return string(upper[:len(op)]), nil
}

func hashSlot(key []byte) int {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	//return int(crc32.ChecksumIEEE(key) % MaxSlotNum)
	return int(hashkit.HashCRC16(key) % MaxSlotNum)
}

func getHashKey(resp *redis.Resp, opstr string) []byte {
	var index = 1
	switch opstr {
	case "ZINTERSTORE", "ZUNIONSTORE", "EVAL", "EVALSHA":
		index = 3
	}
	if index < len(resp.Array) {
		return resp.Array[index].Value
	}
	return nil
}
