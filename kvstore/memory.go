package kvstore

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/patrickmn/go-cache"
)

var (
	MemoryKeyNotFoundError = errors.Errorf("key not found")
)

type MemoryBackend struct {
	c *cache.Cache
}

func NewMemoryBackend() (*MemoryBackend, error) {
	c := cache.New(cache.NoExpiration, cache.NoExpiration)
	return &MemoryBackend{
		c: c,
	}, nil
}

func (m *MemoryBackend) Set(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	m.c.SetDefault(key, string(value))
	return nil
}

func (m *MemoryBackend) Get(key string, obj interface{}) error {
	value, exists := m.c.Get(key)
	if !exists {
		return MemoryKeyNotFoundError
	}
	if err := json.Unmarshal([]byte(value.(string)), obj); err != nil {
		return errors.Wrap(err, "fail to unmarshal json")
	}
	return nil
}

func (m *MemoryBackend) Delete(key string) error {
	m.c.Delete(key)
	return nil
}

func (m *MemoryBackend) Keys(prefix string) ([]string, error) {
	keys := []string{}

	items := m.c.Items()
	for key := range items {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return keys, nil
}

func (m *MemoryBackend) IsNotFoundError(err error) bool {
	return err == MemoryKeyNotFoundError
}
