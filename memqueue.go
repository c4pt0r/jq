package jq

import (
	"container/list"
	"sync"
)

type MemQ struct {
	lst  *list.List
	name string
	lck  sync.Mutex
}

func (q *MemQ) Push(v []byte) error {
	q.lck.Lock()
	defer q.lck.Unlock()
	q.lst.PushBack(v)
	return nil
}

func (q *MemQ) Pop() ([]byte, error) {
	q.lck.Lock()
	defer q.lck.Unlock()

	if q.lst.Len() == 0 {
		return nil, ErrEmpty
	}

	v := q.lst.Front()
	q.lst.Remove(v)
	return v.Value.([]byte), nil
}

func (q *MemQ) Len() int {
	q.lck.Lock()
	q.lck.Unlock()
	return q.lst.Len()
}

func (q *MemQ) Name() string {
	return q.name
}

func MemQFactory(name string) Queue {
	return &MemQ{
		name: name,
		lst:  list.New(),
	}
}
