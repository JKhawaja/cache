package cache

import (
	"testing"
	"time"
)

func TestNewBucket(t *testing.T) {
	cache := NewCache(nil)
	if cache == nil {
		t.Error("new cache not created")
	}

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}
}

func TestBucketAdd(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	err := b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error while adding k/v to cache: %+v", err)
	}

	err = b.Add("key", "value", 10*time.Minute)
	if err != ErrCollision {
		t.Errorf("did not return collision error when adding existing key: %+v", err)
	}
}

func TestBucketDelete(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	err := b.Delete("dne")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}

	err = b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = b.Delete("key")
	if err != nil {
		t.Errorf("error while deleting key: %+v", err)
	}
}

func TestBucketGet(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	_, err := b.Get("dne")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}

	err = b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = b.Delete("key")
	if err != nil {
		t.Errorf("error while deleting key: %+v", err)
	}

	// empty slot
	_, err = b.Get("key")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}
}

func TestBucketUpdate(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	err := b.Update("key", "value")
	if err != ErrDNE {
		t.Errorf("updating key that does not exist did not fail: %+v", err)
	}

	err = b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = b.Update("key", "new-value")
	if err != nil {
		t.Errorf("error while updating key: %+v", err)
	}

	value, err := b.Get("key")
	if err != nil {
		t.Errorf("error while getting key: %+v", err)
	}

	if value.(string) != "new-value" {
		t.Error("value as not updated for key properly")
	}
}

func TestBucketExtend(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	err := b.Extend("key", 1*time.Minute)
	if err != ErrDNE {
		t.Errorf("extending key that does not exist did not fail: %+v", err)
	}

	err = b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = b.Extend("key", 1*time.Minute)
	if err != nil {
		t.Errorf("error while updating key: %+v", err)
	}
}

func TestBucketIterator(t *testing.T) {
	cache := NewCache(nil)

	b := cache.Bucket("my-bucket")
	if b == nil {
		t.Error("bucket was nil")
	}

	err := b.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	if b.Len() == 0 {
		t.Error("length of bucket list is 0")
	}

	iter := b.Iterator()
	i := 0
	for iter.Next() {
		item := iter.Item()
		v, ok := item.(string)
		if !ok {
			t.Error("incorrect item type")
		}

		if v != "value" {
			t.Errorf("returned value was %s", v)
		}

		err := iter.Update("new-value")
		if err != nil {
			t.Errorf("bucket update error: %+v", err)
		}
		i++
	}

	if i == 0 {
		t.Error("iterator did not run over items")
	}

	value, err := b.Get("key")
	if err != nil {
		t.Errorf("error while getting key: %+v", err)
	}

	v, ok := value.(string)
	if !ok {
		t.Error("incorrect item type")
	}

	if v != "new-value" {
		t.Errorf("value was not updated: %s", v)
	}
}
