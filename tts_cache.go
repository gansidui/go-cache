package cache

import (
	"time"
)

type TTLCache struct {
	dataStore  *KVStore
	ttlChecker *TTLChecker
}

func (cache *TTLCache) Open(dataDBPath, ttlDBPath string) error {
	cache.dataStore = &KVStore{}
	cache.ttlChecker = &TTLChecker{}

	if err := cache.dataStore.Open(dataDBPath); err != nil {
		return err
	}
	if err := cache.ttlChecker.Open(ttlDBPath, cache.onKeyExpiredCallback); err != nil {
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

func (c *TTLCache) onKeyExpiredCallback(key []byte) {
	c.dataStore.Delete(key)
}
