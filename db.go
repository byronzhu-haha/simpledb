package bydb

import (
	"byron.com/simpledb/skiplist"
	"math"
	"reflect"
	"sync"
	"time"

	"byron.com/simpledb/errors"
)

var (
	ErrInvalidStringKey        = errors.WithMessage(errors.ErrInvalidKey, "it should be string type")
	ErrInvalidCustomKey        = errors.WithMessage(errors.ErrInvalidKey, "it should be implemented CustomKey interface")
	ErrInvalidCustomKeyPointer = errors.WithMessage(errors.ErrInvalidKey, "it should be value type")
)

type keyType byte

const (
	String keyType = iota + 1
	Custom
)

type CustomKey interface {
	Key() string
}

type Query func(v interface{}) bool

type Config struct {
	withExpired bool
}

type DBOption func(o Config) Config

func DBOptionWithExpired() DBOption {
	return func(o Config) Config {
		o.withExpired = true
		return o
	}
}

type expireCustomKey struct {
	typ        keyType
	expireTime int64
	strKey     string
	key        CustomKey
}

func (k expireCustomKey) Key() string {
	if k.typ == String {
		return k.strKey
	}
	return k.key.Key()
}

type SaveOptions struct {
	isExpired bool
	ttl       int64
}

type SaveOption func(o SaveOptions) SaveOptions

func SaveOptionTTL(ttl int64) SaveOption {
	return func(o SaveOptions) SaveOptions {
		if ttl <= 0 {
			return o
		}
		o.isExpired = true
		o.ttl = ttl
		return o
	}
}

type ByDB struct {
	typ     keyType
	hasInit bool
	mu      sync.RWMutex
	keys    map[string]CustomKey
	data    skiplist.SkipList
	conf    Config
}

// NewCustomByDB 创建一个key可以定制的内存数据库，
// 前提是您的key类型实现了CustomKey接口，且它可比较，即key实例为实现类的值类型
func NewCustomByDB(less func(l, r interface{}) bool, opts ...DBOption) *ByDB {
	var (
		conf = Config{}
	)
	for _, opt := range opts {
		conf = opt(conf)
	}
	db := &ByDB{
		typ:     Custom,
		hasInit: true,
		keys:    make(map[string]CustomKey),
		conf:    conf,
	}
	if conf.withExpired {
		db.data = skiplist.NewSkipList(func(l, r interface{}) bool {
			left := l.(expireCustomKey)
			right := r.(expireCustomKey)
			return less(left.key, right.key)
		})
		go db.background()
	} else {
		db.data = skiplist.NewSkipList(less)
	}
	return db
}

// NewByDB 创建key为string, value为interface{}的内存数据库
func NewByDB(opts ...DBOption) *ByDB {
	var (
		conf = Config{}
	)
	for _, opt := range opts {
		conf = opt(conf)
	}
	db := &ByDB{
		typ:     String,
		hasInit: true,
		conf:    conf,
	}
	less := func(l, r string) bool {
		if l < r {
			return true
		}
		return false
	}
	if db.conf.withExpired {
		db.data = skiplist.NewSkipList(func(l, r interface{}) bool {
			left := l.(expireCustomKey)
			right := r.(expireCustomKey)
			return less(left.strKey, right.strKey)
		})
		db.keys = make(map[string]CustomKey)
		go db.background()
	} else {
		db.data = skiplist.NewSkipList(func(l, r interface{}) bool {
			return less(l.(string), r.(string))
		})
	}
	return db
}

func (d *ByDB) withExpired() bool {
	return d.conf.withExpired
}

func (d *ByDB) checkBeforeOp() {
	if !d.hasInit {
		panic(errors.ErrNotInit)
	}
}

// isValidKey 校验是否为有效的key，目前支持string类型、实现了CustomKey接口的值类型
func (d *ByDB) isValidKey(key interface{}) (CustomKey, error) {
	if key == nil {
		return nil, errors.ErrNilKey
	}
	switch d.typ {
	case String:
		_, ok := key.(string)
		if ok {
			return nil, nil
		}
		return nil, ErrInvalidStringKey
	case Custom:
		cus, ok := key.(CustomKey)
		if ok {
			if cus == nil {
				return nil, errors.ErrNilKey
			}
			typ := reflect.TypeOf(key)
			if typ.Name() == "" {
				return nil, ErrInvalidCustomKeyPointer
			}
			return cus, nil
		}
		return nil, ErrInvalidCustomKey
	}
	return nil, errors.ErrInvalidKey
}

// Save 保存数据，支持过期时间
func (d *ByDB) Save(key, value interface{}, opts ...SaveOption) error {
	// 检测db是否被初始化
	d.checkBeforeOp()

	custom, err := d.isValidKey(key)
	if err != nil {
		return err
	}
	var o SaveOptions
	for _, opt := range opts {
		o = opt(o)
	}
	// 设置过期时，将key包上过期时间
	if d.withExpired() {
		ttl := int64(math.MaxInt64)
		if o.isExpired {
			ttl = o.ttl
		}
		key, custom = d.packWithExpire(key, custom, ttl)
	}
	err = d.data.Set(key, value)
	if err != nil {
		return err
	}
	if custom != nil {
		d.mu.Lock()
		// 删除旧的key
		old := d.keys[custom.Key()]
		if old != nil && old != custom {
			_ = d.data.Del(old)
		}
		d.keys[custom.Key()] = custom
		d.mu.Unlock()
	}
	return nil
}

// packWithExpire 为key包上过期时间
func (d *ByDB) packWithExpire(inKey interface{}, inCustomKey CustomKey, ttl int64) (outKey interface{}, outCustomKey CustomKey) {
	now := time.Now().Unix()
	switch d.typ {
	case String:
		outCustomKey = expireCustomKey{
			typ:        String,
			expireTime: now + ttl,
			strKey:     inKey.(string),
		}
	case Custom:
		outCustomKey = expireCustomKey{
			typ:        Custom,
			expireTime: now + ttl,
			key:        inCustomKey,
		}
	}
	outKey = outCustomKey
	return
}

// Get 根据key获取值，如果是CustomKey类型的key，支持接收CustomKey.Key()作为寻址key
func (d *ByDB) Get(key interface{}) (interface{}, error) {
	// 检测db是否被初始化
	d.checkBeforeOp()

	if !d.withExpired() && d.typ == String {
		return d.data.Get(key)
	}
	custom, err := d.getCustomKey(key, true)
	if err != nil {
		return nil, err
	}
	return d.data.Get(custom)
}

// Delete 删除指定的Key，同Get支持CustomKey.Key()作为寻址key
func (d *ByDB) Delete(key interface{}) error {
	// 检测db是否被初始化
	d.checkBeforeOp()

	if !d.withExpired() && d.typ == String {
		return d.data.Del(key)
	}
	custom, err := d.getCustomKey(key, true)
	if err != nil {
		return err
	}
	err = d.data.Del(custom)
	if err != nil {
		return err
	}
	d.mu.Lock()
	// 双重检测
	custom, err = d.getCustomKey(key, false)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	delete(d.keys, custom.Key())
	d.mu.Unlock()
	return nil
}

func (d *ByDB) getCustomKey(key interface{}, locked bool) (CustomKey, error) {
	var (
		k, ok  = key.(string)
		custom CustomKey
	)

	if ok {
		return d.customKeyRLock(k, locked)
	}

	custom, ok = key.(CustomKey)
	if !ok || custom == nil {
		return nil, errors.ErrNotFound
	}

	if d.withExpired() {
		return d.customKeyRLock(custom.Key(), locked)
	}

	return custom, nil
}

func (d *ByDB) customKeyRLock(key string, locked bool) (custom CustomKey, err error) {
	if locked {
		d.mu.RLock()
		custom = d.keys[key]
		d.mu.RUnlock()
	} else {
		custom = d.keys[key]
	}
	if custom == nil {
		err = errors.ErrNotFound
	}
	return custom, err
}

func (d *ByDB) Count(queries ...Query) (int, error) {
	// 检测db是否被初始化
	d.checkBeforeOp()

	if len(queries) == 0 {
		return d.data.Len(), nil
	}
	var count int
	iter := d.data.Iterator()
	for iter.HasNext() {
		v := iter.Value()
		if v == nil {
			continue
		}
		if !query(v, queries...) {
			continue
		}
		count++
	}
	iter.Close()
	return count, nil
}

func query(v interface{}, queries ...Query) bool {
	for _, q := range queries {
		if !q(v) {
			return false
		}
	}
	return true
}

func (d *ByDB) Iterator() skiplist.Iterator {
	// 检测db是否被初始化
	d.checkBeforeOp()

	return d.data.Iterator()
}

func (d *ByDB) List(page, pageSize int32, queries ...Query) ([]interface{}, bool, error) {
	// 检测db是否被初始化
	d.checkBeforeOp()

	var (
		iter        = d.data.Iterator()
		offset      = (page - 1) * pageSize
		end         = offset + pageSize
		count       int32
		ret         = make([]interface{}, 0, pageSize)
		hasNextPage bool
	)

	// 最坏情况是全表扫描
	for iter.HasNext() {
		if hasNextPage {
			break
		}
		v := iter.Value()
		if v == nil {
			continue
		}
		if !query(v, queries...) {
			continue
		}
		if count >= end {
			hasNextPage = true
			continue
		}
		if offset <= count {
			ret = append(ret, v)
		}
		count++
	}
	iter.Close()
	return ret, hasNextPage, nil
}

func (d *ByDB) background() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		// todo: 如何避免全表扫描
		now := time.Now().Unix()
		iter := d.Iterator()
		for iter.HasNext() {
			ek, ok := iter.Key().(expireCustomKey)
			if !ok {
				continue
			}
			if ek.expireTime <= now {
				err := d.data.Del(ek)
				if err != nil {
					continue
				}
				d.mu.Lock()
				delete(d.keys, ek.Key())
				d.mu.Unlock()
			}
		}
		iter.Close()
	}
}
