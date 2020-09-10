package skiplist

import (
	"fmt"
	"testing"
)

func newList() SkipList {
	return NewSkipList(func(l, r interface{}) bool {
		if l.(string) < r.(string) {
			return true
		}
		return false
	})
}

func TestSkipList_Set(t *testing.T) {
	list := newList()
	err := list.Set("1", 1)
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if list.Len() != 1 {
		t.Errorf("test failed, len should be one")
		return
	}
	err = list.Set("1", 2)
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if list.Len() != 1 {
		t.Errorf("test failed, len should be one")
		return
	}
	err = list.Set("2", 2)
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if list.Len() != 2 {
		t.Errorf("test failed, len should be 2")
		return
	}
}

func TestSkipList_Get(t *testing.T) {
	list := newList()
	_ = list.Set("1", 1)
	v, err := list.Get("1")
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if v != 1 {
		t.Errorf("test failed, v(%+v) should be 1", v)
		return
	}
	_ = list.Set("1", 2)
	v, err = list.Get("1")
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if v != 2 {
		t.Errorf("test failed, v(%+v) should be 2", v)
	}
}

func TestSkipList_Del(t *testing.T) {
	list := newList()
	_ = list.Set("1", 1)
	err := list.Del("1")
	if err != nil {
		t.Errorf("test failed, err: %+v", err)
		return
	}
	if list.Len() != 0 {
		t.Errorf("test failed, len should be 0")
		return
	}
	v, err := list.Get("1")
	if err != ErrNotFound || v != nil {
		t.Errorf("test failed, err should be ErrNotFound or v should be nil")
	}
}

func TestSkipList_Iterator(t *testing.T) {
	list := newList()
	in := []string{"1", "1", "6", "6", "3", "3", "2", "2", "4", "4"}
	exp := []string{"1", "1", "2", "2", "3", "3", "4", "4", "6", "6"}
	for i := 0; i < len(in); i += 2 {
		err := list.Set(in[i], in[i+1])
		t.Logf("%s %s", in[i], in[i+1])
		if err != nil {
			t.Errorf("err: %+v", err)
		}
	}
	fmt.Println(list.Len())
	for i := 0; i < len(exp); i += 2 {
		fmt.Println(list.Get(exp[i]))
	}
	iter := list.Iterator()
	var out []string
	for iter.HasNext() {
		out = append(out, iter.Key().(string), iter.Value().(string))
	}
	iter.Close()
	fmt.Println(out)
	if len(out) != len(exp) {
		t.Errorf("test failed, len of out(%d) should be len of exp", len(out))
		return
	}
	for i, s := range out {
		if s != exp[i] {
			t.Errorf("test failed, s(%+v) should be equal exp[%d](%+v)", s, i, exp[i])
			return
		}
	}

}
