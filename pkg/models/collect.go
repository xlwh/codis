package models

import (
	"encoding/json"
	"fmt"

	"github.com/wandoulabs/zkhelper"
)

type Meta struct {
	Slots   map[string]*Slot        `json:"slots"`
	Groups  map[string]*ServerGroup `json:"groups"`
	Epic    uint64                  `json:"epic"`
	Product string                  `json:"product"`
}

type Epic struct {
	V uint64 `json:"epic"`
}

func GetEpicPath(product string) string {
	return fmt.Sprintf("/zk/codis/db_%s/epic", product)
}

func GetEpic(zkConn zkhelper.Conn, product string) (uint64, error) {
	b, _, err := zkConn.Get(GetEpicPath(product))
	if err != nil {
		return 0, err
	}
	e := &Epic{}
	if err := json.Unmarshal(b, e); err != nil {
		return 0, err
	}
	return e.V, nil
}
