package jq

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"
)

var ErrTimeout = errors.New("timeout")

type Job struct {
	Id      string        `json:"id"`
	Data    []byte        `json:"data"`
	Timeout time.Duration `json:"timeout"`
	onRet   func([]byte)
	onErr   func(error)
}

type MsgType int

const (
	MSG_RET MsgType = iota
	MSG_DONE
	MSG_ERR
)

type Msg struct {
	Type MsgType `json:"type"`
	Data []byte  `json:"data"`
}

var globalId int32 = 1

func genId() string {
	i := atomic.AddInt32(&globalId, 1)
	return strconv.Itoa(int(i))
}
