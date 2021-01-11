package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"io/ioutil"
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

// Slot is a slot in a cache
type Slot struct {
	Item      interface{}
	ExpiresAt time.Time
	empty     bool
}

// Cache is a generic in-memory cache
type Cache struct {
	slots   []Slot
	keys    map[uint64]int
	nextExp time.Time
	config  *CacheConfig
	*sync.Mutex
}

// OnExpires is a function that will act on the item object
// of an expired Slot.
type OnExpires func(item interface{})

// CacheConfig is used to configure a cache
type CacheConfig struct {
	OnExpires       OnExpires
	Refresh         bool // extends key's expiration time on usage (for lru-like behavior)
	RefreshDuration time.Duration
	CleanDuration   time.Duration
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

// Get will return the value stored at the key.
// It will return an ErrDNE value if key is not in cache.
func (t *Cache) Get(key string) (interface{}, error) {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return nil, err
	}
	hashedKey := hasher.Sum64()
	idx, ok := t.keys[hashedKey]
	if !ok {
		return nil, ErrDNE
	}

	item := t.slots[idx]
	if item.empty {
		delete(t.keys, hashedKey)
		return nil, ErrDNE
	}

	if t.config.Refresh {
		err := t.Extend(key, t.config.RefreshDuration)
		if err != nil {
			return nil, err
		}
	}

	return item.Item, nil
}

// Delete will delete a key from the cache.
// It will return ErrDNE if the key does not exist.
func (t *Cache) Delete(key string) error {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()
	idx, ok := t.keys[hashedKey]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].empty = true
	delete(t.keys, hashedKey)

	return nil
}

// Update updates the value at the key to the new supplied value
func (t *Cache) Update(key string, item interface{}) error {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	idx, ok := t.keys[hashedKey]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].Item = item

	return nil
}

// Extend will extend the time until expiration for the specified key by the specified duration.
func (t *Cache) Extend(key string, extend time.Duration) error {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()

	idx, ok := t.keys[hashedKey]
	if !ok {
		return ErrDNE
	}

	t.slots[idx].ExpiresAt = t.slots[idx].ExpiresAt.Add(extend)

	if t.nextExp.After(t.slots[idx].ExpiresAt) {
		t.nextExp = t.slots[idx].ExpiresAt
	}

	return nil
}

// Add will add a key, value, and expiration duration to the cache.
// If the key already exists in the collision (i.e. if a collision occurs) then an
// ErrCollision value will be returned.
func (t *Cache) Add(key string, item interface{}, expiresIn time.Duration) error {
	t.Lock()
	defer t.Unlock()

	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return err
	}
	hashedKey := hasher.Sum64()
	_, ok := t.keys[hashedKey]
	if ok {
		return ErrCollision
	}

	expiresAt := time.Now().UTC().Add(expiresIn)
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

	t.keys[hashedKey] = idx

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

// Load --
func (c *Cache) Load(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	return c.GobDecode(data)
}

// Save --
func (c *Cache) Save(filename string) error {
	data, err := c.GobEncode()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, data, 0777)
}

// GobEncode --
func (c *Cache) GobEncode() ([]byte, error) {
	var buff bytes.Buffer
	e := gob.NewEncoder(&buff)
	err := e.Encode(c)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

// GobDecode --
func (c *Cache) GobDecode(data []byte) error {
	var buf bytes.Buffer
	_, err := buf.Write(data)
	if err != nil {
		return err
	}

	d := gob.NewDecoder(&buf)
	return d.Decode(c)
}
