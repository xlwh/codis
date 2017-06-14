// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/YongMan/codis/pkg/models"
	"github.com/YongMan/codis/pkg/utils"
	"github.com/YongMan/codis/pkg/utils/errors"
	"github.com/YongMan/codis/pkg/utils/log"
)

const (
	METHOD_GET    HttpMethod = "GET"
	METHOD_POST   HttpMethod = "POST"
	METHOD_PUT    HttpMethod = "PUT"
	METHOD_DELETE HttpMethod = "DELETE"
)

type HttpMethod string

func jsonify(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func callApi(method HttpMethod, apiPath string, params interface{}, retVal interface{}) error {
	if apiPath[0] != '/' {
		return errors.Errorf("api path must starts with /")
	}
	url := "http://" + globalEnv.DashboardAddr() + apiPath
	client := &http.Client{Transport: http.DefaultTransport}

	b, err := json.Marshal(params)
	if err != nil {
		return errors.Trace(err)
	}

	req, err := http.NewRequest(string(method), url, strings.NewReader(string(b)))
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("can't connect to dashboard, please check 'dashboard_addr' is corrent in config file")
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}

	if resp.StatusCode == 200 {
		err := json.Unmarshal(body, retVal)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("http status code %d, %s", resp.StatusCode, string(body))
}

func getMeta(product string, servers []string) (*models.Meta, error) {
	h := servers[rand.Int()%len(servers)]
	c, err := net.DialTimeout("tcp", h, time.Second)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.Close()

	epic := make([]byte, 8)
	/* always trigger update */
	utils.PutUint64(epic, 0)
	if _, err := c.Write(epic); err != nil {
		return nil, errors.Trace(err)
	}

	b, err := ioutil.ReadAll(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b == nil || len(b) == 0 {
		return nil, fmt.Errorf("no data return, prossible error")
	}

	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	m := &models.Meta{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, errors.Trace(err)
	}

	/* check product */
	if m.Product != globalEnv.ProductName() {
		return nil, fmt.Errorf("invailidate product: %s", m.Product)
	}
	return m, nil
}
