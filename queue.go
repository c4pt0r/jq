package jq

import "errors"

var ErrEmpty = errors.New("queue is empty")

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Len() int
	Name() string
}

type QueueFactory func(name string) Queue
