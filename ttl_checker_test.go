package cache

import (
	"os"
	"testing"
	"time"
)

type TTLCheckerForTest struct {
	t                 *testing.T
	checker           *TTLChecker
	keys              []string
	ttlArray          []int64
	expectExpiredkeys []string
	idx               int
}

func (c *TTLCheckerForTest) OnKeyExpiredCallback(info *TTLInfo) {
	if string(info.Key) != c.expectExpiredkeys[c.idx] {
		c.t.Fatal()
	}
	// ttlInfo must equal info
	ttlInfo, err := c.checker.GetInfo(info.Key)
	if err != nil || ttlInfo.Less(info) || info.Less(ttlInfo) {
		c.t.Fatal()
	}
	c.idx++
}

func (c *TTLCheckerForTest) Open(dbPath string) {
	c.checker.Open(dbPath, c.OnKeyExpiredCallback)

	info, err := c.checker.GetInfo([]byte("not_exist_key"))
	if err == nil || info != nil {
		c.t.Fatal()
	}

	for i := 0; i < len(c.keys); i++ {
		var createTime int64 = time.Now().Unix()
		c.checker.SetTTL([]byte(c.keys[i]), createTime, c.ttlArray[i])

		info, err := c.checker.GetInfo([]byte(c.keys[i]))
		if err != nil || info.CreateTime != createTime || info.TTL != c.ttlArray[i] {
			c.t.Fatal()
		}
	}
}

func (c *TTLCheckerForTest) Close() {
	c.checker.Close()
}

func (c *TTLCheckerForTest) WaitAndClose(waitSecond int) {
	time.Sleep(time.Duration(waitSecond) * time.Second)
	c.checker.Close()
}

func (c *TTLCheckerForTest) Count() uint64 {
	return c.checker.db.Count()
}

func TestTTLChecker(t *testing.T) {
	dbPath := "test.db"
	defer os.RemoveAll(dbPath)

	checker := &TTLCheckerForTest{
		t:                 t,
		checker:           &TTLChecker{},
		keys:              []string{"1", "3", "2", "6", "5", "4"},
		ttlArray:          []int64{6, 5, 3, 4, 1, 2},
		expectExpiredkeys: []string{"5", "4", "2", "6", "3", "1"},
	}
	checker.Open(dbPath)

	if checker.Count() != 6 {
		t.Fatal()
	}

	checker.WaitAndClose(10)

	if checker.Count() != 0 {
		t.Fatal()
	}

	checker = &TTLCheckerForTest{
		t:        t,
		checker:  &TTLChecker{},
		keys:     []string{"1", "3", "2"},
		ttlArray: []int64{1, 3, 2},
	}
	checker.Open(dbPath)
	checker.checker.SetTTL([]byte("1"), time.Now().Unix(), 4)

	if checker.Count() != 3 {
		t.Fatal()
	}

	checker.expectExpiredkeys = []string{"2", "3", "1"}

	checker.WaitAndClose(6)

	if checker.Count() != 0 {
		t.Fatal()
	}
}
