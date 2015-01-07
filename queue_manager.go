package jq

import "sync"

type QueueManager interface {
	Exists(name string) (bool, error)
	Get(name string) (Queue, error)
	Del(name string) error
}

type QueueManagerFactory func(QueueFactory) QueueManager

/* memory only queue manager implment  */

type MemQueueManager struct {
	queues   map[string]Queue
	qfactory QueueFactory
	lck      sync.Mutex
}

func MemQueueManagerFactory(qfactory QueueFactory) *MemQueueManager {
	return &MemQueueManager{
		queues:   make(map[string]Queue),
		qfactory: qfactory,
	}
}

func (self *MemQueueManager) exists(name string) bool {
	_, b := self.queues[name]
	return b
}

func (self *MemQueueManager) Exists(name string) (bool, error) {
	self.lck.Lock()
	defer self.lck.Unlock()
	return self.exists(name), nil
}

func (self *MemQueueManager) Get(name string) (Queue, error) {
	self.lck.Lock()
	defer self.lck.Unlock()

	if !self.exists(name) {
		self.queues[name] = self.qfactory(name)
	}
	return self.queues[name], nil
}

func (self *MemQueueManager) Del(name string) error {
	self.lck.Lock()
	defer self.lck.Unlock()
	if self.exists(name) {
		delete(self.queues, name)
	}
	return nil
}
