package cache

import (
	"hash/fnv"
	"time"
)

// Bucket indexes a group of keys in cache
// and should be used to manage them
type Bucket struct {
	name  string
	list  []uint64
	cache *Cache
}

type bucketIterator struct {
	bucket   *Bucket
	key      uint64
	item     interface{}
	position int
}

// Bucket will return the bucket if it exists.
// It will create and return a new bucket by the name
// if the bucket does not already exist.
func (c *Cache) Bucket(name string) *Bucket {
	obj, err := c.Get(name)
	if err == ErrDNE {
		b := &Bucket{
			name:  name,
			list:  make([]uint64, 0),
			cache: c,
		}

		err := c.Add(name, b, 0)
		if err != nil {
			return nil
		}

		return b
	} else if err != nil {
		return nil
	}

	return obj.(*Bucket)
}

// Add will add an item to the bucket.
func (b *Bucket) Add(key string, item interface{}, expiresIn time.Duration) error {
	b.cache.Lock()
	defer b.cache.Unlock()

	pk := b.name + "-" + key
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(pk))
	if err != nil {
		return err
	}
	hk := hasher.Sum64()

	var exists bool
	for _, k := range b.list {
		if k == hk {
			exists = true
			break
		}
	}

	if !exists {
		b.list = append(b.list, hk)
	}

	expiresAt := time.Now().UTC().Add(expiresIn)
	return b.cache.add(hk, item, expiresAt)
}

// Delete will remove an item from the bucket
func (b *Bucket) Delete(key string) error {
	b.cache.Lock()
	defer b.cache.Unlock()

	pk := b.name + "-" + key
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(pk))
	if err != nil {
		return err
	}
	hk := hasher.Sum64()

	for i, k := range b.list {
		if k == hk {
			b.list = append(b.list[:i], b.list[i+1:]...)
			break
		}
	}

	return b.cache.delete(hk)
}

// Get will get an item from the bucket.
func (b *Bucket) Get(key string) (interface{}, error) {
	b.cache.Lock()
	defer b.cache.Unlock()

	pk := b.name + "-" + key
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(pk))
	if err != nil {
		return nil, err
	}
	hk := hasher.Sum64()

	return b.cache.get(hk)
}

// Extend will extend an item from the bucket.
func (b *Bucket) Extend(key string, extend time.Duration) error {
	b.cache.Lock()
	defer b.cache.Unlock()

	pk := b.name + "-" + key
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(pk))
	if err != nil {
		return err
	}
	hk := hasher.Sum64()

	return b.cache.extend(hk, extend)
}

// Iterator will return an iterator to iterate
// over the items in the bucket.
func (b *Bucket) Iterator() *bucketIterator {
	return &bucketIterator{
		bucket: b,
	}
}

// Len returns the number of items in the bucket
func (b *Bucket) Len() int {
	return len(b.list)
}

// Update will update the item in the bucket
func (b *Bucket) Update(key string, item interface{}) error {
	b.cache.Lock()
	defer b.cache.Unlock()

	pk := b.name + "-" + key
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(pk))
	if err != nil {
		return err
	}
	hk := hasher.Sum64()

	return b.cache.update(hk, item)
}

/*  bucket iterator */

// Item will return the current item that the
// iterator has retrieved from the bucket.
func (b *bucketIterator) Item() interface{} {
	return b.item
}

// Next will return false when there
// are no items remaining to iterate
func (b *bucketIterator) Next() bool {
	if b.position < len(b.bucket.list) {
		key := b.bucket.list[b.position]
		item, _ := b.bucket.cache.get(key)

		b.key = key
		b.item = item

		b.position++
		return true
	}

	return false
}

// Update will update the object currently in the iterator,
// which can be checked with the `Item()` method,
// with the provided item object in the argument.
func (b *bucketIterator) Update(item interface{}) error {
	b.bucket.cache.Lock()
	defer b.bucket.cache.Unlock()

	return b.bucket.cache.update(b.key, item)
}
