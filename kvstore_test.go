package cache

import (
	"bytes"
	"os"
	"strconv"
	"testing"
)

func TestBasic(t *testing.T) {
	dbPath := "test.db"
	defer os.RemoveAll(dbPath)

	db := &KVStore{}
	err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Put([]byte("aa_key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err = db.Put([]byte("ab_key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	if err = db.Put([]byte("bb_key3"), []byte("value3")); err != nil {
		t.Fatal(err)
	}
	if err = db.Put([]byte("abb_key4"), []byte("value4")); err != nil {
		t.Fatal(err)
	}

	if db.Count() != 4 {
		t.Fatal()
	}

	_, err = db.Get([]byte("none"))
	if err == nil {
		t.Fatal()
	}

	keys := db.Next([]byte{}, 100)
	if len(keys) != 4 {
		t.Fatal()
	}
	if string(keys[0]) != "aa_key1" || string(keys[3]) != "bb_key3" {
		t.Fatal()
	}

	v, err := db.Get([]byte("bb_key3"))
	if err != nil || !bytes.Equal(v, []byte("value3")) {
		t.Fatal(err)
	}
	if !db.Has([]byte("bb_key3")) {
		t.Fatal()
	}

	if err = db.Delete([]byte("abb_key4")); err != nil {
		t.Fatal(err)
	}
	if db.Count() != 3 {
		t.Fatal()
	}
	if db.Has([]byte("abb_key4")) {
		t.Fatal()
	}

	keys = db.Next(nil, 100)
	if len(keys) != 3 {
		t.Fatal()
	}

	for _, key := range keys {
		err = db.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
	}

	if db.Has([]byte("bb_key3")) {
		t.Fatal()
	}

	if db.Count() != 0 {
		t.Fatal()
	}
}

func TestNext(t *testing.T) {
	dbPath := "test.db"
	defer os.RemoveAll(dbPath)

	db := &KVStore{}
	err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	limit := 10000
	for i := 0; i < limit; i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i))

		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}

		vv, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(vv, value) {
			t.Fatal()
		}
	}
	if db.Count() != uint64(limit) {
		t.Fatal()
	}

	keys := db.Next([]byte(""), 1)
	if !bytes.Equal(keys[0], []byte("0")) {
		t.Fatal()
	}

	keys = db.Next([]byte("0"), 2)
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("1")) || !bytes.Equal(keys[1], []byte("10")) {
		t.Fatal()
	}

	keys = db.Next([]byte("999999999"), 1)
	if len(keys) != 0 {
		t.Fatal()
	}

	keys = db.Next([]byte("101"), 1)
	if !bytes.Equal(keys[0], []byte("1010")) {
		t.Fatal()
	}

	keys = db.Next([]byte("1009"), 1)
	if !bytes.Equal(keys[0], []byte("101")) {
		t.Fatal()
	}

	keys = db.Next([]byte("2339"), 3)
	if len(keys) != 3 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("234")) || !bytes.Equal(keys[1], []byte("2340")) ||
		!bytes.Equal(keys[2], []byte("2341")) {
		t.Fatal()
	}

	db.Delete([]byte("2340"))
	keys = db.Next([]byte("2339"), 3)
	if !bytes.Equal(keys[0], []byte("234")) || !bytes.Equal(keys[1], []byte("2341")) ||
		!bytes.Equal(keys[2], []byte("2342")) {
		t.Fatal()
	}

	keys = db.Next([]byte("9997"), 10)
	if len(keys) != 2 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("9998")) || !bytes.Equal(keys[1], []byte("9999")) {
		t.Fatal()
	}
}

func TestPrev(t *testing.T) {
	dbPath := "test.db"
	defer os.RemoveAll(dbPath)

	db := &KVStore{}
	err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	limit := 20
	for i := 10; i < limit; i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i))

		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}
	keys := db.Next(nil, 1)
	if !bytes.Equal(keys[0], []byte("10")) {
		t.Fatal()
	}

	keys = db.Next([]byte("17"), 5)
	if len(keys) != 2 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("18")) || !bytes.Equal(keys[1], []byte("19")) {
		t.Fatal()
	}

	keys = db.Prev([]byte{}, 1)
	if !bytes.Equal(keys[0], []byte("19")) {
		t.Fatal()
	}
	keys = db.Prev(nil, 2)
	if len(keys) != 2 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("19")) || !bytes.Equal(keys[1], []byte("18")) {
		t.Fatal()
	}

	keys = db.Prev([]byte("19"), 3)
	if len(keys) != 3 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("18")) || !bytes.Equal(keys[1], []byte("17")) ||
		!bytes.Equal(keys[2], []byte("16")) {
		t.Fatal()
	}

	keys = db.Prev([]byte("222"), 3)
	if len(keys) != 3 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("19")) || !bytes.Equal(keys[1], []byte("18")) ||
		!bytes.Equal(keys[2], []byte("17")) {
		t.Fatal()
	}

	keys = db.Prev([]byte("11"), 30)
	if len(keys) != 1 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("10")) {
		t.Fatal()
	}

	keys = db.Prev([]byte("10"), 1)
	if len(keys) != 0 {
		t.Fatal()
	}

	// 注意 9 的字典序在这里是最大的
	keys = db.Prev([]byte("9"), 3)
	if len(keys) != 3 {
		t.Fatal()
	}
	if !bytes.Equal(keys[0], []byte("19")) || !bytes.Equal(keys[1], []byte("18")) ||
		!bytes.Equal(keys[2], []byte("17")) {
		t.Fatal()
	}
}

func TestSequence(t *testing.T) {
	dbPath := "test.db"
	defer os.RemoveAll(dbPath)

	db := &KVStore{}
	err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	seq := db.CurrentSequence()
	if seq != 0 {
		t.Fatal()
	}
	seq, err = db.NextSequence()
	if err != nil || seq != 1 || db.CurrentSequence() != 1 {
		t.Fatal()
	}

	for i := 1; i <= 1000; i++ {
		if db.CurrentSequence() != uint64(i) {
			t.Fatal()
		}

		seq, err = db.NextSequence()
		if err != nil || seq != uint64(i+1) {
			t.Fatal()
		}
	}
}
