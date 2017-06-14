// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"bytes"
	"sync"

	"github.com/ksarch-saas/codis/pkg/proxy/redis"
	"github.com/ksarch-saas/codis/pkg/utils/atomic2"
)

type Dispatcher interface {
	Dispatch(r *Request) error
	DispatchAsking(r *Request, hkey []byte) error
}

type Request struct {
	OpStr string
	Start int64
	Write bool

	Resp *redis.Resp

	Coalesce func() error
	Response struct {
		Resp *redis.Resp
		Err  error
	}

	Wait *sync.WaitGroup
	slot *sync.WaitGroup

	Failed *atomic2.Bool

	Target string
}

func (r *Request) isAskResp() bool {
	return r.Response.Resp.IsError() && bytes.Compare(r.Response.Resp.Value, []byte("ASK")) == 0
}

func (r *Request) isAskingReq() bool {
	return r.Resp.IsString() && bytes.Compare(r.Resp.Value, []byte("ASKING")) == 0
}

func (r *Request) isPongResp() bool {
	return bytes.Compare(r.Response.Resp.Value, []byte("PONG")) == 0
}
