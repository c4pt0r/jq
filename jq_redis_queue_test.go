package jq

import (
	"bytes"
	"runtime"
	"testing"

	log "github.com/ngaut/logging"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("info")
}

func TestRedisEnqueue(t *testing.T) {
	jq := NewJq("test_queue1", RedisQueueManagerFactory(RedisQueueFactory), MockWorkerFunc)
	go jq.DispatchForever()
	jq.Submit([]byte("hello"), func(ret []byte) {
		log.Info("i am from redis", string(ret))
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}
