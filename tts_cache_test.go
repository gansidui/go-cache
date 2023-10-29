package cache

import (
	"os"
	"testing"
	"time"
)

func TestTTLCache(t *testing.T) {
	dataDBPath := "data.db"
	ttlDBPath := "ttl.db"

	defer func() {
		os.RemoveAll(dataDBPath)
		os.RemoveAll(ttlDBPath)
	}()

	cache := &TTLCache{}
	if err := cache.Open(dataDBPath, ttlDBPath); err != nil {
		t.Fatal(err)
	}

	cache.Put([]byte("key1"), []byte("value1"), 3)
	if cache.Count() != 1 {
		t.Fatal()
	}

	cache.Put([]byte("key2"), []byte("value2"), 2)
	if cache.Count() != 2 {
		t.Fatal()
	}
	cache.Put([]byte("key2"), []byte("new_value2"), 10)

	data, err := cache.Get([]byte("key1"))
	if err != nil || string(data) != "value1" {
		t.Fatal()
	}

	time.Sleep(5 * time.Second)

	data, err = cache.Get([]byte("key1"))
	if err == nil {
		t.Fatal()
	}

	data, err = cache.Get([]byte("key2"))
	if err != nil || string(data) != "new_value2" {
		t.Fatal()
	}
}
