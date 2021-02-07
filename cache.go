package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"io/ioutil"
	"math"
	"sync"
	"time"
)

var (
	// ErrCollision is a hash collision error
	ErrCollision = errors.New("hash collision")
	// ErrDNE is a "does not exist" error
	ErrDNE = errors.New("does not exist")

	defaultConfig = &CacheConfig{
		CleanDuration: defaultCleanDuration,
	}
	defaultCleanDuration   = 10 * time.Second
	defaultRefreshDuration = 1 * time.Second
)

// Cache is a generic in-memory cache
type Cache struct {
	slots   []Slot
	keys    map[uint64]int
	nextExp time.Time
	config  *CacheConfig
	*sync.Mutex
}

// CacheConfig is used to configure a cache
type CacheConfig struct {
	OnExpires       OnExpires
	Refresh         bool // extends key's expiration time on usage (for lru-like behavior)
	RefreshDuration time.Duration
	CleanDuration   time.Duration
}

// OnExpires is a function that will act on the item object
// of an expired Slot.
type OnExpires func(item interface{})

// Slot is a slot in a cache
type Slot struct {
	Item      interface{}
	ExpiresAt time.Time
	empty     bool
}

// NewCache will create and return a pointer to a new Cache object
// Renewable sets whether
func NewCache(config *CacheConfig) *Cache {
	if config == nil {
		config = defaultConfig
	}

	if config.CleanDuration == 0 {
		config.CleanDuration = defaultCleanDuration
	}

	if config.Refresh {
		if config.RefreshDuration == 0 {
			config.RefreshDuration = defaultRefreshDuration
		}
	}

	t := &Cache{
		slots:  make([]Slot, 0),
		keys:   make(map[uint64]int),
		config: config,
		Mutex:  &sync.Mutex{},
	}

	go func(t *Cache) {
		for {
			time.Sleep(t.config.CleanDuration)
			if time.Now().UTC().After(t.nextExp) {
				for _, exp := range t.clean() {
					t.config.OnExpires(exp.Item)
				}
			}
		}
	}(t)

	return t
}

// Add will add a key, value, and expiration duration to the cache.
// If the key already exists in the collision (i.e. if a collision occurs) then an
// ErrCollision value will be returned.
// If you use an expiresIn time of `0` then the item will never be expired from the cache.
func (t *Cache) Add(key string, item interface{}, expiresIn time.Duration) error {
	t.Lock()
	defer t.Unlock()

	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	var expiresAt time.Time
	if expiresIn == 0 {
		expiresAt = time.Unix(math.MaxInt64, 0)
	} else {
		expiresAt = time.Now().UTC().Add(expiresIn)
	}

	return t.add(hashedKey, item, expiresAt)
}

// Delete will delete a key from the cache.
// It will return ErrDNE if the key does not exist.
func (t *Cache) Delete(key string) error {
	t.Lock()
	defer t.Unlock()

	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	return t.delete(hashedKey)
}

// Extend will extend the time until expiration for the specified key by the specified duration.
func (t *Cache) Extend(key string, extend time.Duration) error {
	t.Lock()
	defer t.Unlock()

	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	return t.extend(hashedKey, extend)
}

// Get will return the value stored at the key.
// It will return an ErrDNE value if key is not in cache.
func (t *Cache) Get(key string) (interface{}, error) {
	t.Lock()
	defer t.Unlock()

	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return nil, err
	}
	hashedKey := hasher.Sum64()

	return t.get(hashedKey)
}

// Load will load an empty cache with the data from
// the given file. File should contain a gob encoded
// cached object created via the `Save()` method.
func (c *Cache) Load(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	return c.gobDecode(data)
}

// Save will gob-encode and persist the cache
// in its current state to a file of the given name.
func (c *Cache) Save(filename string) error {
	data, err := c.gobEncode()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, data, 0777)
}

// Update updates the value at the key to the new supplied value
func (t *Cache) Update(key string, item interface{}) error {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	return t.update(hashedKey, item)
}

func (t *Cache) add(key uint64, item interface{}, expiresAt time.Time) error {
	_, ok := t.keys[key]
	if ok {
		return ErrCollision
	}

	ts := Slot{
		Item:      item,
		ExpiresAt: expiresAt,
		empty:     false,
	}

	var idx int
	var fit bool
	for i, c := range t.slots {
		if c.empty {
			t.slots[i] = ts
			fit = true
			idx = i
		}
	}
	if !fit {
		idx = len(t.slots)
		t.slots = append(t.slots, ts)
	}

	if t.nextExp.After(expiresAt) || len(t.slots) == 1 {
		t.nextExp = expiresAt
	}

	t.keys[key] = idx

	return nil
}

func (t *Cache) clean() []Slot {
	t.Lock()
	defer t.Unlock()

	var expired []Slot
	var nearestExp time.Time
	firstNonEmpty := true
	for i, object := range t.slots {
		if !object.empty {
			if time.Now().UTC().After(object.ExpiresAt) {
				expired = append(expired, object)
				t.slots[i].empty = true
			} else {
				if firstNonEmpty {
					nearestExp = object.ExpiresAt
					firstNonEmpty = false
				}

				if nearestExp.After(object.ExpiresAt) {
					nearestExp = object.ExpiresAt
				}
			}
		}
	}

	t.nextExp = nearestExp

	return expired
}

func (t *Cache) delete(key uint64) error {
	idx, ok := t.keys[key]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].empty = true
	delete(t.keys, key)

	return nil
}

func (t *Cache) extend(key uint64, extend time.Duration) error {
	idx, ok := t.keys[key]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].ExpiresAt = t.slots[idx].ExpiresAt.Add(extend)

	if t.nextExp.After(t.slots[idx].ExpiresAt) {
		t.nextExp = t.slots[idx].ExpiresAt
	}

	return nil
}

func (t *Cache) get(key uint64) (interface{}, error) {
	idx, ok := t.keys[key]
	if !ok {
		return nil, ErrDNE
	}

	item := t.slots[idx]
	if item.empty {
		delete(t.keys, key)
		return nil, ErrDNE
	}

	if t.config.Refresh {
		err := t.extend(key, t.config.RefreshDuration)
		if err != nil {
			return nil, err
		}
	}

	return item.Item, nil
}

func (c *Cache) gobEncode() ([]byte, error) {
	var buff bytes.Buffer
	e := gob.NewEncoder(&buff)
	err := e.Encode(c)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (c *Cache) gobDecode(data []byte) error {
	var buf bytes.Buffer
	_, err := buf.Write(data)
	if err != nil {
		return err
	}

	d := gob.NewDecoder(&buf)
	return d.Decode(c)
}

func (t *Cache) update(key uint64, item interface{}) error {
	idx, ok := t.keys[key]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].Item = item

	return nil
}
