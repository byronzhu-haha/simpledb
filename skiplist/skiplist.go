package skiplist

import (
	"byron.com/simpledb/errors"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32
	probability = 0.5
)

type SkipList interface {
	Set(key, value interface{}) error
	Get(key interface{}) (value interface{}, err error)
	Del(key interface{}) error
	Len() int
	Iterator() Iterator
}

type skipList struct {
	head     *node
	len      int
	maxLevel int
	less     func(l, r interface{}) bool
	mu       sync.RWMutex
}

func NewSkipList(less func(l, r interface{}) bool) SkipList {
	return &skipList{
		head: &node{
			forward: []*node{nil},
		},
		maxLevel: maxLevel,
		less:     less,
	}
}

// level 当前跳表的最高层级
func (s *skipList) level() int {
	return len(s.head.forward) - 1
}

// maxEffectiveLevel 跳表当前允许的最大有效层级
func (s *skipList) maxEffectiveLevel() int {
	if s.maxLevel > s.level() {
		return s.maxLevel
	}
	return s.level()
}

// getNewLevel 获取一个新的层级(随机选择)
func (s *skipList) getNewLevel() int {
	rand.Seed(time.Now().UnixNano())
	level := 0
	for level < s.maxEffectiveLevel() && rand.Float64() < probability {
		level++
	}
	return level
}

// addressing 从source节点开始寻址，找到符合key的节点
func (s *skipList) addressing(key interface{}, source *node, beUpdatedNodes []*node) (*node, error) {
	var (
		height  = len(source.forward) - 1
		current = source
	)
	if beUpdatedNodes != nil && len(beUpdatedNodes) < height {
		return nil, errors.ErrUpdateSliceIdxOutRange
	}
	for i := height; i >= 0; i-- {
		for current.forward[i] != nil && s.less(current.forward[i].key, key) {
			current = current.forward[i]
		}
		if beUpdatedNodes != nil {
			beUpdatedNodes[i] = current
		}
	}
	return current.next(), nil
}

// Set 设置新值，根据less函数，保证每次插入都是有序的
func (s *skipList) Set(key, value interface{}) error {
	if key == nil {
		return errors.ErrNilKey
	}

	s.mu.Lock()
	// 存在该key，直接更新
	update := make([]*node, s.level()+1, s.maxEffectiveLevel()+1)
	dest, err := s.addressing(key, s.head, update)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if dest != nil && dest.key == key {
		dest.value = value
		s.mu.Unlock()
		return nil
	}

	level := s.getNewLevel()
	for i := s.level() + 1; i <= level; i++ {
		update = append(update, s.head)
		s.head.forward = append(s.head.forward, nil)
	}

	// 长度加1
	s.len++

	newNode := &node{
		forward: make([]*node, level+1, s.maxEffectiveLevel()+1),
		key:     key,
		value:   value,
	}

	if backward := update[0]; backward.key != nil {
		newNode.backward = backward
	}

	for i := 0; i <= level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	if newNode.forward[0] != nil && newNode.forward[0].backward != newNode {
		newNode.forward[0].backward = newNode
	}

	s.mu.Unlock()

	return nil
}

func (s *skipList) Get(key interface{}) (value interface{}, err error) {
	if key == nil {
		return nil, errors.ErrNilKey
	}
	s.mu.RLock()
	dest, err := s.addressing(key, s.head, nil)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	if dest == nil || dest.key != key {
		s.mu.RUnlock()
		return nil, errors.ErrNotFound
	}
	s.mu.RUnlock()
	return dest.value, nil
}

func (s *skipList) Del(key interface{}) error {
	if key == nil {
		return errors.ErrNilKey
	}

	s.mu.RLock()
	update := make([]*node, s.level()+1, s.maxEffectiveLevel())
	dest, err := s.addressing(key, s.head, update)
	if err != nil {
		s.mu.RUnlock()
		return err
	}
	if dest == nil || dest.key != key {
		s.mu.RUnlock()
		return errors.ErrNotFound
	}
	s.mu.RUnlock()

	s.mu.Lock()
	// 双重检测
	update = make([]*node, s.level()+1, s.maxEffectiveLevel())
	dest, err = s.addressing(key, s.head, update)
	if err != nil {
		s.mu.Unlock()
		return nil
	}
	if dest == nil || dest.key != key {
		s.mu.Unlock()
		return errors.ErrNotFound
	}

	// 长度减1
	s.len--

	next := dest.next()
	if next != nil {
		next.backward = dest.backward
	}

	// 删除所有层的dest
	for i := 0; i <= s.level() && update[i].forward[i] == dest; i++ {
		update[i].forward[i] = dest.forward[i]
	}
	// 删去空层
	for s.level() > 0 && s.head.forward[s.level()] == nil {
		s.head.forward = s.head.forward[:s.level()]
	}

	s.mu.Unlock()

	return nil
}

func (s *skipList) Iterator() Iterator {
	s.mu.RLock()
	iter := &iterator{
		currNode: s.head,
	}
	s.mu.RUnlock()
	return iter
}

func (s *skipList) Len() int {
	s.mu.RLock()
	length := s.len
	s.mu.RUnlock()
	return length
}
