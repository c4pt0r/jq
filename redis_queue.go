package jq

import "github.com/fzzy/radix/redis"

type RedisQueue struct {
	c    *redis.Client
	name string
}

func NewRedisQueue(name string) *RedisQueue {
	client, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	return &RedisQueue{
		c:    client,
		name: name,
	}
}

func (q *RedisQueue) Push(b []byte) error {
	_, err := q.c.Cmd("LPUSH", string(q.name), b).Int()
	return err
}

func (q *RedisQueue) Pop() ([]byte, error) {
	if q.Len() == 0 {
		return nil, ErrEmpty
	}
	return q.c.Cmd("RPOP", q.name).Bytes()
}

func (q *RedisQueue) Len() int {
	l, err := q.c.Cmd("LLEN", q.name).Int()
	if err != nil {
		return -1
	}
	return l
}

func (q *RedisQueue) Name() string {
	return q.name
}

func RedisQueueFactory(name string) Queue {
	r := NewRedisQueue(name)
	return r
}
