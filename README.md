# go-cache

Wraps leveldb and implements TTL.


## Usage

```
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	cache "github.com/gansidui/go-cache"
)

func main() {
	dataDBPath := "data.db"
	ttlDBPath := "ttl.db"

	defer func() {
		os.RemoveAll(dataDBPath)
		os.RemoveAll(ttlDBPath)
	}()

	cache := &cache.TTLCache{}
	if err := cache.Open(dataDBPath, ttlDBPath); err != nil {
		log.Fatal(err)
	}

	// Set TTL of 3 seconds
	cache.Put([]byte("key"), []byte("value"), 3)

	if value, err := cache.Get([]byte("key")); err == nil {
		fmt.Println(string(value)) // output: value
	}

	// Wait longer than TTL
	time.Sleep(5 * time.Second)

	if value, err := cache.Get([]byte("key")); err != nil {
		fmt.Println(err) // output: not found
	}
}

```


