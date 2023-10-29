package cache

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// 注意：
// leveldb.OpenFile 返回的对象是线程安全的，详见：https://github.com/syndtr/goleveldb
// 需要加锁是因为要维护 keyForCount、 keyForSequence 等成员变量，只需要在读写成员变量的地方加读写锁即可。
// 另外，leveldb.Get 等接口返回的字节数组是不允许修改的，为了安全，最好是先拷贝一份再返回

var (
	// 由于leveldb没有接口获取key的数量，所以需要自己维护一个key来存储key的总数
	keyForCount = []byte("__key_for_count__")

	// 用于序号递增
	keyForSequence = []byte("__key_for_sequence__")

	// 内部保留key不允许被外界直接读取
	reservedlKeys = make([][]byte, 0)
)

func init() {
	reservedlKeys = append(reservedlKeys, keyForCount)
	reservedlKeys = append(reservedlKeys, keyForSequence)
}

func isReservedlKey(key []byte) bool {
	for _, reservedlKey := range reservedlKeys {
		if bytes.Equal(key, reservedlKey) {
			return true
		}
	}
	return false
}

type KVStore struct {
	db     *leveldb.DB
	dbPath string

	// 保护 keyForCount、keyForSequence 等成员变量的读写
	mutex sync.RWMutex
}

func (kv *KVStore) Open(path string) error {
	var err error
	if kv.db, err = leveldb.OpenFile(path, nil); err != nil {
		return errors.New(fmt.Sprintf("KVStore open [%v] failed: %v", path, err))
	}
	kv.dbPath = path
	log.Printf("KVStore open [%v] success\n", path)
	return nil
}

func (kv *KVStore) Close() error {
	log.Printf("KVStore close [%v]\n", kv.dbPath)
	return kv.db.Close()
}

func (kv *KVStore) Put(key, value []byte) error {
	if isReservedlKey(key) {
		return errors.New("Not allow put reserved key")
	}

	if kv.Has(key) {
		return kv.db.Put(key, value, nil)
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// key总数+1
	count := kv.count() + 1

	// 需要使用批处理，同时更新两个key
	batch := new(leveldb.Batch)
	batch.Put(key, value)
	batch.Put(keyForCount, []byte(strconv.FormatUint(count, 10)))

	return kv.db.Write(batch, nil)
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	if isReservedlKey(key) {
		return nil, errors.New("Not allow get reserved key")
	}

	value, err := kv.db.Get(key, nil)
	if err == nil {
		copyValue := make([]byte, len(value))
		copy(copyValue, value)
		return copyValue, nil
	}
	return nil, err
}

func (kv *KVStore) Delete(key []byte) error {
	if isReservedlKey(key) {
		return errors.New("Not allow delete reserved key")
	}

	if kv.Has(key) {
		kv.mutex.Lock()
		defer kv.mutex.Unlock()

		// key总数-1
		count := kv.count() - 1

		// 需要使用批处理，同时更新两个key
		batch := new(leveldb.Batch)
		batch.Delete(key)
		batch.Put(keyForCount, []byte(strconv.FormatUint(count, 10)))

		return kv.db.Write(batch, nil)
	}

	return errors.New("Key not exist")
}

func (kv *KVStore) Has(key []byte) bool {
	exist, err := kv.db.Has(key, nil)
	if err == nil && exist {
		return true
	}
	return false
}

// 返回指定key后面的n个key（不包括当前key，当前key也可以不存在）
// 如果当前key为空数组或者nil，表示从头开始遍历
// 如果当前key为字典序最大，则返回的结果为空；如果当前key为字典序最小，则返回最前的n个key
// 注意：leveldb 是根据 key 的字典序排序的
func (kv *KVStore) Next(key []byte, n int) [][]byte {
	keys := make([][]byte, 0)
	iter := kv.db.NewIterator(nil, nil)

	ok := false
	if key == nil || bytes.Equal(key, []byte("")) {
		ok = iter.First()
	} else {
		ok = iter.Seek(key)
	}

	for ; ok && len(keys) < n; ok = iter.Next() {
		// 过滤当前key 和 保留key
		if bytes.Equal(iter.Key(), key) || isReservedlKey(iter.Key()) {
			continue
		}

		copyKey := make([]byte, len(iter.Key()))
		copy(copyKey, iter.Key())
		keys = append(keys, copyKey)
	}
	iter.Release()

	return keys
}

// 返回指定key前面的n个key（不包括当前key，当前key也可以不存在）
// 如果当前key为空数组或者nil，表示从末尾开始遍历
// 如果当前key为字典序最小，则返回的结果为空；如果当前key为字典序最大，则返回最后的n个key
// 注意：leveldb 是根据 key 的字典序排序的
func (kv *KVStore) Prev(key []byte, n int) [][]byte {
	keys := make([][]byte, 0)
	iter := kv.db.NewIterator(nil, nil)

	ok := false
	if key == nil || bytes.Equal(key, []byte("")) {
		ok = iter.Last()
	} else {
		ok = iter.Seek(key)
	}

	for ; ok && len(keys) < n; ok = iter.Prev() {
		// 过滤当前key 和 保留key
		if bytes.Equal(iter.Key(), key) || isReservedlKey(iter.Key()) {
			continue
		}

		copyKey := make([]byte, len(iter.Key()))
		copy(copyKey, iter.Key())
		keys = append(keys, copyKey)
	}
	iter.Release()

	return keys
}

// 返回 key 的数量
func (kv *KVStore) Count() uint64 {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	return kv.count()
}

func (kv *KVStore) count() uint64 {
	if !kv.Has(keyForCount) {
		return 0
	}

	value, err := kv.db.Get(keyForCount, nil)
	if err != nil {
		return 0
	}

	count, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0
	}
	return count
}

// 返回当前Sequence
func (kv *KVStore) CurrentSequence() uint64 {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	return kv.currentSequence()
}

func (kv *KVStore) currentSequence() uint64 {
	if !kv.Has(keyForSequence) {
		return 0
	}

	value, err := kv.db.Get(keyForSequence, nil)
	if err != nil {
		return 0
	}

	sequence, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0
	}
	return sequence
}

// 生成并返回下一个Sequence
// 注意这是一个读写操作
func (kv *KVStore) NextSequence() (uint64, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	sequence := kv.currentSequence() + 1
	err := kv.db.Put(keyForSequence, []byte(strconv.FormatUint(sequence, 10)), nil)

	return sequence, err
}
