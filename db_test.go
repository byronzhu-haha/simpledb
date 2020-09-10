package simpledb

import (
	"github.com/byronzhu-haha/simpledb/errors"
	"strconv"
	"testing"
	"time"
)

var db = NewDB()

func TestByDB_Save(t *testing.T) {
	err := db.Save("2", 2)
	if err != nil {
		t.Errorf("save failed, err should be nil, err: %+v", err)
		return
	}
	if err = db.Save("2", 3); err != nil {
		t.Errorf("save failed, err should be nil, err: %+v", err)
		return
	}
	if db.data.Len() != 1 {
		t.Errorf("save failed, len of db should be 1")
		return
	}
	if v, _ := db.data.Get("2"); v != 3 {
		t.Errorf("save failed, v should be 3")
	}
}

func TestByDB_Count(t *testing.T) {
	for i := 0; i < 10; i++ {
		_ = db.Save(strconv.Itoa(i), i)
	}
	count, err := db.Count()
	if err != nil {
		t.Errorf("count failed, err should be nil")
		return
	}
	if count != 10 {
		t.Errorf("count failed, count should be 10")
	}
}

func TestByDB_Get(t *testing.T) {
	_ = db.Save("5", "10")
	v, err := db.Get("5")
	if err != nil {
		t.Errorf("get failed, err should be nil")
		return
	}
	if v != "10" {
		t.Errorf("get failed, v should be \"10\"")
	}
}

func TestByDB_Delete(t *testing.T) {
	_ = db.Save("5", 10)
	err := db.Delete("5")
	if err != nil {
		t.Errorf("del failed, err should be nil")
		return
	}
	if count, _ := db.Count(); count != 0 {
		t.Errorf("del failed, left item should be zero")
		return
	}
	if _, err = db.Get("5"); err != errors.ErrNotFound {
		t.Errorf("del failed, err of got item should be ErrNil")
	}
}

func TestByDB_Iterator(t *testing.T) {
	in := map[string]int{
		"1": 1,
		"2": 2,
		"3": 3,
		"4": 4,
	}
	exp := []int{1, 2, 3, 4}
	for s, i := range in {
		_ = db.Save(s, i)
	}
	iter := db.Iterator()
	var out []int
	for iter.HasNext() {
		out = append(out, iter.Value().(int))
	}
	for i, o := range out {
		if o != exp[i] {
			t.Errorf("iterate failed, o should be exp[i]")
			return
		}
	}
}

func TestByDB_List(t *testing.T) {
	in := map[string]int{
		"1": 1,
		"2": 2,
		"3": 3,
		"4": 4,
		"5": 5,
		"6": 6,
	}
	exp := []int{1, 2, 3, 4}
	for s, i := range in {
		_ = db.Save(s, i)
	}
	query := func(v interface{}) bool {
		if v.(int) <= 4 {
			return true
		}
		return false
	}
	out, hasNextPage, err := db.List(1, 10, query)
	if err != nil {
		t.Errorf("list failed, err should be nil")
		return
	}
	if hasNextPage {
		t.Errorf("list failed, has not next page")
		return
	}
	for i, o := range out {
		if o != exp[i] {
			t.Errorf("list failed, o should be exp[i]")
			return
		}
	}
}

var expiredDB = NewDB(DBOptionWithExpired())

func TestByDBExpired_Save(t *testing.T) {
	err := expiredDB.Save("1", 1, SaveOptionTTL(2))
	if err != nil {
		t.Errorf("save failed, err should be nil, err: %+v", err)
		return
	}
	v, err := expiredDB.Get("1")
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if v != 1 {
		t.Errorf("test failed, v should be 1")
		return
	}
	time.Sleep(3 * time.Second)
	v, err = expiredDB.Get("1")
	if err != errors.ErrNotFound {
		t.Errorf("test failed, err should be ErrNil")
	}
}

type customKey struct {
	key string
	seq int32
}

func (k customKey) Key() string {
	return k.key
}

func newCustomKey(key string, seq int32) CustomKey {
	return customKey{
		key: key,
		seq: seq,
	}
}

var customDB = NewCustomDB(func(l, r interface{}) bool {
	left := l.(customKey)
	right := r.(customKey)
	if left.seq < right.seq {
		return true
	}
	return false
})

type value struct {
	key   string
	seq   int32
	score int32
}

func TestCustomDB_Save(t *testing.T) {
	err := customDB.Save(struct{}{}, 33)
	if err == nil {
		t.Errorf("save failed, err should not be nil")
		return
	} else {
		t.Logf("err: %+v", err)
	}
	k, v := newKV()
	err = customDB.Save(&k, v)
	if err == nil {
		t.Errorf("save failed, err should not be nil")
		return
	} else {
		t.Logf("err: %+v", err)
	}
	err = customDB.Save(k, v)
	if err != nil {
		t.Errorf("save failed, err should be nil")
		return
	}
	if customDB.data.Len() != 1 {
		t.Errorf("save failed, len of db should be 1")
	}
}

func newKV() (k CustomKey, v value) {
	v = value{
		key:   "1",
		seq:   1,
		score: 99,
	}
	k = newCustomKey(v.key, v.seq)
	return
}

func TestCustomDB_Get(t *testing.T) {
	k, v := newKV()
	_ = customDB.Save(k, v)
	nv, err := customDB.Get(k)
	if err != nil {
		t.Errorf("get failed, err should be err")
		return
	}
	if nv != v {
		t.Errorf("get failed, nv should be equal v")
		return
	}
	nv, err = customDB.Get("1")
	if err != nil {
		t.Errorf("get failed, err should be err")
		return
	}
	if nv != v {
		t.Errorf("get failed, nv should be equal v")
	}
}

func TestCustomDB_Count(t *testing.T) {
	_ = customDB.Save(newKV())
	c, err := customDB.Count()
	if err != nil {
		t.Errorf("count failed, err should be nil")
		return
	}
	if c != 1 {
		t.Errorf("count failed, c should be 1")
	}
}

func TestCustomDB_Delete(t *testing.T) {
	k, v := newKV()
	_ = customDB.Save(k, v)
	err := customDB.Delete(k)
	if err != nil {
		t.Errorf("delete failed, err should be nil")
		return
	}
	if n, _ := customDB.Count(); n != 0 {
		t.Errorf("delete failed, err should be 0")
		return
	}
	if _, err = customDB.Get(k); err != errors.ErrNotFound {
		t.Errorf("delete failed, err should be ErrNil")
	}
}

func TestCustomDB_Iterator(t *testing.T) {
	vs := newKVN()
	kvs := map[CustomKey]value{}
	for _, v := range vs {
		kvs[newCustomKey(v.key, v.seq)] = v
	}
	for key, v := range kvs {
		_ = customDB.Save(key, v)
	}
	var (
		out  []value
		iter = customDB.Iterator()
	)
	for iter.HasNext() {
		out = append(out, iter.Value().(value))
	}
	for i, o := range out {
		if o != vs[i] {
			t.Errorf("iterate failed, o should be vs[i]")
			return
		}
	}
}

func newKVN() []value {
	vs := []value{
		{
			key:   "1",
			seq:   1,
			score: 10,
		},
		{
			key:   "2",
			seq:   2,
			score: 15,
		},
		{
			key:   "3",
			seq:   3,
			score: 18,
		},
		{
			key:   "4",
			seq:   4,
			score: 20,
		},
	}

	return vs
}

func TestCustomDB_List(t *testing.T) {
	vs := newKVN()
	for _, v := range vs {
		_ = customDB.Save(newCustomKey(v.key, v.seq), v)
	}
	exp := []value{
		{
			key:   "1",
			seq:   1,
			score: 10,
		},
		{
			key:   "2",
			seq:   2,
			score: 15,
		},
	}
	out, hasNextPage, err := customDB.List(1, 1, func(v interface{}) bool {
		vv, ok := v.(value)
		if !ok {
			return false
		}
		if vv.score > 15 {
			return false
		}
		return true
	})
	if err != nil {
		t.Errorf("list failed, err should be nil")
		return
	}
	if !hasNextPage {
		t.Errorf("list failed, has not next page")
		return
	}
	for i, o := range out {
		if o != exp[i] {
			t.Errorf("list failed, o should be exp[i]")
			return
		}
	}
}

var expiredCustomDB = NewCustomDB(func(l, r interface{}) bool {
	left := l.(customKey)
	right := r.(customKey)
	if left.seq < right.seq {
		return true
	}
	return false
}, DBOptionWithExpired())

func TestCustomDBExpired_Save(t *testing.T) {
	k, v := newKV()
	err := expiredCustomDB.Save(k, v, SaveOptionTTL(2))
	if err != nil {
		t.Errorf("save failed, err should be nil")
		return
	}
	if l := expiredCustomDB.data.Len(); l != 1 {
		t.Errorf("save failed, len of db should be 1, l: %+v", l)
		return
	}
	nv, err := expiredCustomDB.Get(k)
	if err != nil {
		t.Errorf("save failed, err should be nil")
		return
	}
	if nv != v {
		t.Errorf("save failed, nv should be equal v")
		return
	}
	time.Sleep(3 * time.Second)
	if l := expiredCustomDB.data.Len(); l != 0 {
		t.Errorf("save failed, len of db should be 0, l: %d", l)
	}
}
