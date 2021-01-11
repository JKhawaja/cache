package cache

import (
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(nil)
	if cache == nil {
		t.Error("new cache not created")
	}
}

func TestCacheDelete(t *testing.T) {
	cache := NewCache(nil)
	err := cache.Delete("dne")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}

	err = cache.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = cache.Delete("key")
	if err != nil {
		t.Errorf("error while deleting key: %+v", err)
	}
}

func TestCacheGet(t *testing.T) {
	cache := NewCache(nil)
	_, err := cache.Get("dne")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}

	err = cache.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = cache.Delete("key")
	if err != nil {
		t.Errorf("error while deleting key: %+v", err)
	}

	// empty slot
	_, err = cache.Get("key")
	if err != ErrDNE {
		t.Errorf("should have returned ErrDNE but returned %+v", err)
	}

}

func TestCacheUpdate(t *testing.T) {
	cache := NewCache(nil)
	err := cache.Update("key", "value")
	if err != ErrDNE {
		t.Errorf("updating key that does not exist did not fail: %+v", err)
	}

	err = cache.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = cache.Update("key", "new-value")
	if err != nil {
		t.Errorf("error while updating key: %+v", err)
	}

	value, err := cache.Get("key")
	if err != nil {
		t.Errorf("error while getting key: %+v", err)
	}

	if value.(string) != "new-value" {
		t.Error("value as not updated for key properly")
	}
}

func TestCacheExtend(t *testing.T) {
	cache := NewCache(nil)
	err := cache.Extend("key", 1*time.Minute)
	if err != ErrDNE {
		t.Errorf("extending key that does not exist did not fail: %+v", err)
	}

	err = cache.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error adding key: %+v", err)
	}

	err = cache.Extend("key", 1*time.Minute)
	if err != nil {
		t.Errorf("error while updating key: %+v", err)
	}
}

func TestCacheAdd(t *testing.T) {
	cache := NewCache(nil)
	err := cache.Add("key", "value", 10*time.Minute)
	if err != nil {
		t.Errorf("error while adding k/v to cache: %+v", err)
	}

	err = cache.Add("key", "value", 10*time.Minute)
	if err != ErrCollision {
		t.Errorf("did not return collision error when adding existing key: %+v", err)
	}
}
