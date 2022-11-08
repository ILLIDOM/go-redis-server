package main

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

var mutex = &sync.RWMutex{}

type Storage struct {
	data map[string]RedisValue
}

type RedisValue struct {
	value      string
	expiration time.Time
}

func (rv RedisValue) IsExpired() bool {
	if rv.expiration.IsZero() {
		return false
	}
	return rv.expiration.Before(time.Now())
}

func InitStorage() *Storage {
	return &Storage{
		data: make(map[string]RedisValue),
	}
}

func (s *Storage) SetKey(key, value string) error {
	rv := RedisValue{
		value:      value,
		expiration: time.Time{}, //zero time -> 01.01.0001 00:00:00 UTC
	}

	mutex.Lock()
	s.data[key] = rv
	mutex.Unlock()

	return nil
}

func (s *Storage) SetKeyWithExpiration(key, value, px string) error {
	pxInt, err := strconv.Atoi(px)
	if err != nil {
		fmt.Println("Error converting px string to px integer")
		return err
	}

	rv := RedisValue{
		value:      value,
		expiration: time.Now().Add(time.Millisecond * time.Duration(pxInt)),
	}

	mutex.Lock()
	s.data[key] = rv
	mutex.Unlock()

	return nil
}

func (s *Storage) GetValue(key string) (string, error) {
	mutex.RLock()
	rv, ok := s.data[key]
	mutex.RUnlock()

	// check if no expiration is set
	if ok && !rv.IsExpired() {
		return rv.value, nil
	}

	return "", errors.New("key not present")
}
