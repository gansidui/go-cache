package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gansidui/skiplist"
)

type TTLInfo struct {
	Key         []byte `json:"key"`          // 唯一Key
	CreateTime  int64  `json:"create-time"`  // 创建时间戳，单位：秒
	TTL         int64  `json:"ttl"`          // 生命周期，单位：秒
	ExpiredTime int64  `json:"expired-time"` // 过期时间戳，时间单位：秒，该值等于 CreateTime + TTL
}

func (info *TTLInfo) Less(other interface{}) bool {
	if info.ExpiredTime < other.(*TTLInfo).ExpiredTime {
		return true
	}
	if info.ExpiredTime == other.(*TTLInfo).ExpiredTime &&
		bytes.Compare(info.Key, other.(*TTLInfo).Key) < 0 {
		return true
	}
	return false
}

// 注意：回调中不能修改 info
type KeyExpiredCallback = func(info *TTLInfo)

type TTLChecker struct {
	callback      KeyExpiredCallback
	skList        *skiplist.SkipList
	stopChan      chan bool
	checkInterval time.Duration
	db            *KVStore
	mutex         sync.RWMutex
}

func (c *TTLChecker) Open(path string, callback KeyExpiredCallback) error {
	c.callback = callback
	c.skList = skiplist.New()
	c.stopChan = make(chan bool)
	c.checkInterval = time.Duration(time.Second)
	c.db = &KVStore{}
	if err := c.db.Open(path); err != nil {
		return err
	}

	c.loadAllData()
	c.startCheckLoop()

	return nil
}

func (c *TTLChecker) Close() error {
	c.stopChan <- true
	close(c.stopChan)
	return c.db.Close()
}

func (c *TTLChecker) loadAllData() {
	keyCount := c.db.Count()
	allKeyBytes := c.db.Next(nil, int(keyCount))

	for _, key := range allKeyBytes {
		if value, err := c.db.Get(key); err == nil {
			info := &TTLInfo{}
			if err = json.Unmarshal(value, &info); err == nil {
				c.addIndex(info, false)
			}
		}
	}

	log.Printf("TTLChecker load data finished, key count: %v\n", keyCount)
}

func (c *TTLChecker) startCheckLoop() {
	ticker := time.NewTicker(c.checkInterval)

	go func(ticker *time.Ticker) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.checkExpired()
			case <-c.stopChan:
				log.Println("TTLChecker check loop is stopped")
				return
			}
		}
	}(ticker)
}

func (c *TTLChecker) checkExpired() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now().Unix()
	for element := c.skList.Front(); element != nil; element = element.Next() {
		// 由于 skList 是有序的，所以只要从头开始检查，遇到没过期的立即跳出即可
		info := element.Value.(*TTLInfo)
		if info.ExpiredTime >= now {
			break
		}
		// TTL触发回调，注意：先回调再删除索引，避免中途进程挂了
		if c.callback != nil {
			c.callback(info)
		}
		c.deleteIndex(info)
	}
}

// 设置 key 的生命周期（TTL），可以用于新增或者更新，TTL单位：秒
// 注意：createTime 和 ttl 大于 0 才生效
func (c *TTLChecker) SetTTL(key []byte, createTime, ttl int64) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if createTime <= 0 || ttl <= 0 {
		return errors.New(fmt.Sprintf("createTime[%v] or ttl[%v] invalid", createTime, ttl))
	}

	info, err := c.GetInfo(key)
	if err == nil {
		if info.CreateTime == createTime && info.TTL == ttl {
			return nil
		}
		c.deleteIndex(info)
	}

	info = &TTLInfo{
		Key:         key,
		CreateTime:  createTime,
		TTL:         ttl,
		ExpiredTime: createTime + ttl,
	}

	return c.addIndex(info, true)
}

func (c *TTLChecker) Delete(key []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	info, err := c.GetInfo(key)
	if err != nil {
		return err
	}

	return c.deleteIndex(info)
}

func (c *TTLChecker) GetInfo(key []byte) (*TTLInfo, error) {
	value, err := c.db.Get(key)
	if err != nil {
		return nil, err
	}

	info := &TTLInfo{}
	if err = json.Unmarshal(value, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (c *TTLChecker) GetKeyCount() uint64 {
	return c.db.Count()
}

func (c *TTLChecker) addIndex(info *TTLInfo, writeDB bool) error {
	c.skList.Insert(info)

	if writeDB {
		value, err := json.Marshal(info)
		if err != nil {
			return err
		}
		if err := c.db.Put(info.Key, value); err != nil {
			return err
		}
	}

	return nil
}

func (c *TTLChecker) deleteIndex(info *TTLInfo) error {
	c.skList.Delete(info)
	return c.db.Delete(info.Key)
}
