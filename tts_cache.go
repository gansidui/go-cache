package cache

import (
	"time"
)

type TTLCache struct {
	dataStore  *KVStore
	ttlChecker *TTLChecker
}

func (c *TTLCache) Open(dataDBPath, ttlDBPath string) error {
	c.dataStore = &KVStore{}
	c.ttlChecker = &TTLChecker{}

	if err := c.dataStore.Open(dataDBPath); err != nil {
		return err
	}
	if err := c.ttlChecker.Open(ttlDBPath, c.onKeyExpiredCallback); err != nil {
		return err
	}
	return nil
}

func (c *TTLCache) Close() {
	c.dataStore.Close()
	c.ttlChecker.Close()
}

func (c *TTLCache) Put(key, value []byte, ttl int64) error {
	if err := c.dataStore.Put(key, value); err != nil {
		return err
	}
	if err := c.ttlChecker.SetTTL(key, time.Now().Unix(), ttl); err != nil {
		return err
	}
	return nil
}

func (c *TTLCache) Get(key []byte) ([]byte, error) {
	return c.dataStore.Get(key)
}

func (c *TTLCache) Count() uint64 {
	return c.dataStore.count()
}

func (c *TTLCache) onKeyExpiredCallback(info *TTLInfo) {
	c.dataStore.Delete(info.Key)
}
