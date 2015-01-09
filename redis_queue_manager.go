package jq

import (
	"sync"

	"github.com/fzzy/radix/redis"
)

type RedisQueueManager struct {
	c        *redis.Client
	qfactory QueueFactory
	lck      sync.Mutex
}

func (m *RedisQueueManager) Exists(name string) (bool, error) {
	m.lck.Lock()
	defer m.lck.Unlock()
	return m.c.Cmd("SISMEMBER", "queues", name).Bool()
}

func (m *RedisQueueManager) Get(name string) (Queue, error) {
	m.lck.Lock()
	defer m.lck.Unlock()
	b, err := m.c.Cmd("SISMEMBER", "queues", name).Bool()
	if err != nil {
		return nil, err
	}
	if !b {
		return nil, ErrNotExists
	}
	return m.qfactory(name), nil
}

func (m *RedisQueueManager) GetOrCreate(name string) (Queue, error) {
	m.lck.Lock()
	defer m.lck.Unlock()
	m.c.Cmd("SADD", "queues", name)
	return m.qfactory(name), nil
}

func (m *RedisQueueManager) Del(name string) error {
	m.lck.Lock()
	defer m.lck.Unlock()
	_, err := m.c.Cmd("SREM", "queues", name).Bool()
	return err
}

func RedisQueueManagerFactory(qfactory QueueFactory) QueueManager {
	client, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	return &RedisQueueManager{
		c:        client,
		qfactory: qfactory,
	}
}
