// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/docopt/docopt-go"

	"github.com/YongMan/codis/pkg/models"
	"github.com/YongMan/codis/pkg/utils/errors"
	"github.com/YongMan/codis/pkg/utils/log"
)

func cmdSlot(argv []string) (err error) {
	usage := `usage:
	codis-config slot init [-f]
	codis-config slot clean [-f]
	codis-config slot info <slot_id>
	codis-config slot set <slot_id> <group_id> <status>
	codis-config slot range-set <slot_from> <slot_to> <group_id> <status>
	codis-config slot migrate <slot_from> <slot_to> <group_id> [--delay=<delay_time_in_ms>] [--speed=<speed_in_M>] [--timeout=<time_in_ms>]
`

	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		log.ErrorErrorf(err, "parse args failed")
		return errors.Trace(err)
	}
	log.Debugf("parse args = {%+v}", args)

	// no need to lock here
	// locked in runmigratetask
	if args["migrate"].(bool) {
		delay := 0
		groupId, err := strconv.Atoi(args["<group_id>"].(string))
		if err != nil {
			log.ErrorErrorf(err, "parse <group_id> failed")
			return errors.Trace(err)
		}
		if args["--delay"] != nil {
			delay, err = strconv.Atoi(args["--delay"].(string))
			if err != nil {
				log.ErrorErrorf(err, "parse <--delay> failed")
				return errors.Trace(err)
			}
		}
		slotFrom, err := strconv.Atoi(args["<slot_from>"].(string))
		if err != nil {
			log.ErrorErrorf(err, "parse <slot_from> failed")
			return errors.Trace(err)
		}

		slotTo, err := strconv.Atoi(args["<slot_to>"].(string))
		if err != nil {
			log.ErrorErrorf(err, "parse <slot_to> failed")
			return errors.Trace(err)
		}
		speed := 2000
		if args["--speed"] != nil {
			speed, err = strconv.Atoi(args["--speed"].(string))
			if err != nil {
				log.ErrorErrorf(err, "parse <--speed> failed")
				return errors.Trace(err)
			}
		}
		timeout := 5000
		if args["--timeout"] != nil {
			timeout, err = strconv.Atoi(args["--timeout"].(string))
			if err != nil {
				log.ErrorErrorf(err, "parse <--timeout> failed")
				return errors.Trace(err)
			}
		}

		return runSlotMigrate(slotFrom, slotTo, groupId, delay, speed, timeout)
	}

	if args["init"].(bool) {
		force := args["-f"].(bool)
		return runSlotInit(force)
	}

	if args["clean"].(bool) {
		force := args["-f"].(bool)
		return runSlotClean(force)
	}

	if args["info"].(bool) {
		slotId, err := strconv.Atoi(args["<slot_id>"].(string))
		if err != nil {
			return runSlotRangeInfo()
		}
		return runSlotInfo(slotId)
	}

	groupId, err := strconv.Atoi(args["<group_id>"].(string))
	if err != nil {
		log.ErrorErrorf(err, "parse <group_id> failed")
		return errors.Trace(err)
	}

	if args["set"].(bool) {
		slotId, err := strconv.Atoi(args["<slot_id>"].(string))
		status := args["<status>"].(string)
		if err != nil {
			log.ErrorErrorf(err, "parse <slot_id> failed")
			return errors.Trace(err)
		}
		return runSlotSet(slotId, groupId, status)
	}

	if args["range-set"].(bool) {
		status := args["<status>"].(string)
		slotFrom, err := strconv.Atoi(args["<slot_from>"].(string))
		if err != nil {
			log.ErrorErrorf(err, "parse <slot_from> failed")
			return errors.Trace(err)
		}
		slotTo, err := strconv.Atoi(args["<slot_to>"].(string))
		if err != nil {
			log.ErrorErrorf(err, "parse <slot_to> failed")
			return errors.Trace(err)
		}
		return errors.Trace(runSlotRangeSet(slotFrom, slotTo, groupId, status))
	}
	return nil
}

func runSlotInit(isForce bool) error {
	var v interface{}
	url := "/api/slots/init"
	if isForce {
		url += "?is_force=1"
	}
	err := callApi(METHOD_POST, url, nil, &v)
	if err != nil {
		return errors.Trace(err)
	}
	fmt.Println(jsonify(v))
	return nil
}

func runSlotClean(isForce bool) error {
	var v interface{}
	url := "/api/slots/clean"
	if isForce {
		url += "?is_force=1"
	}
	err := callApi(METHOD_POST, url, nil, &v)
	if err != nil {
		return errors.Trace(err)
	}
	fmt.Println(jsonify(v))
	return nil
}

func runSlotInfo(slotId int) error {
	var v interface{}
	err := callApi(METHOD_GET, fmt.Sprintf("/api/slot/%d", slotId), nil, &v)
	if err != nil {
		return errors.Trace(err)
	}
	fmt.Println(jsonify(v))
	return nil
}

type SlotSlice struct {
	Slots []*models.Slot
}

func (s SlotSlice) Len() int {
	return len(s.Slots)
}

func (s SlotSlice) Less(i, j int) bool {
	if s.Slots[i].GroupId == s.Slots[j].GroupId {
		return s.Slots[i].Id < s.Slots[j].Id
	}
	return s.Slots[i].GroupId < s.Slots[j].GroupId
}

func (s SlotSlice) Swap(i, j int) {
	s.Slots[i], s.Slots[j] = s.Slots[j], s.Slots[i]
}

func runSlotRangeInfo() error {
	var slots SlotSlice
	err := callApi(METHOD_GET, "/api/slots", nil, &(slots.Slots))
	if err != nil {
		return errors.Trace(err)
	}
	sort.Sort(slots)
	size := len(slots.Slots)
	if size == 0 {
		return errors.New("no slot")
	}
	prev_gid := -1
	start := -1
	stop := -1
	for _, s := range slots.Slots {
		if prev_gid != s.GroupId {
			if prev_gid != -1 {
				fmt.Printf("%5d\t[%5d %5d]\n", prev_gid, start, stop)
			}
			prev_gid = s.GroupId
			start = s.Id
			stop = s.Id
		} else if s.Id != stop+1 {
			if stop != -1 {
				fmt.Printf("%5d\t[%5d %5d]\n", prev_gid, start, stop)
			}
			start = s.Id
			stop = s.Id
		} else {
			stop++
		}
	}
	fmt.Printf("%5d\t[%5d %5d]\n", slots.Slots[size-1].GroupId, start, stop)
	return nil
}

func runSlotRangeSet(fromSlotId, toSlotId int, groupId int, status string) error {
	t := RangeSetTask{
		FromSlot:   fromSlotId,
		ToSlot:     toSlotId,
		NewGroupId: groupId,
		Status:     status,
	}

	var v interface{}
	err := callApi(METHOD_POST, "/api/slot", t, &v)
	if err != nil {
		return errors.Trace(err)
	}
	fmt.Println(jsonify(v))
	return nil
}

func runSlotSet(slotId int, groupId int, status string) error {
	return runSlotRangeSet(slotId, slotId, groupId, status)
}

func runSlotMigrate(fromSlotId, toSlotId int, newGroupId int, delay, speed, timeout int) error {
	migrateInfo := &migrateTaskForm{
		From:    fromSlotId,
		To:      toSlotId,
		Group:   newGroupId,
		Delay:   delay,
		Speed:   speed,
		Timeout: timeout,
	}
	var v interface{}
	err := callApi(METHOD_POST, "/api/migrate", migrateInfo, &v)
	if err != nil {
		return err
	}
	fmt.Println(jsonify(v))
	return nil
}

func runRebalance(delay int) error {
	/*var v interface{}
	err := callApi(METHOD_POST, "/api/rebalance", nil, &v)
	if err != nil {
		return err
	}
	fmt.Println(jsonify(v)) */
	fmt.Println("deprecated")
	return nil
}
