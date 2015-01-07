package jq

import (
	"strconv"
	"sync/atomic"
)

type Job struct {
	Id      string        `json:"id"`
	Data    []byte        `json:"data"`
	RetChan chan []byte   `json:"-"`
	Done    chan struct{} `json:"-"`
	ErrChan chan error    `json:"-"`
}

var globalId int32 = 1

func genId() string {
	i := atomic.AddInt32(&globalId, 1)
	return strconv.Itoa(int(i))
}
