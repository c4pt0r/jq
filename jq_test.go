package jq

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	log "github.com/ngaut/logging"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("info")
}

func MockWorkerFunc(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
	ret <- []byte("world")
	done <- struct{}{}
}

func TestEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MockWorkerFunc, nil)
	jq.Submit([]byte("hello"), func(ret []byte) {
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}

func TestCocurrentEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MockWorkerFunc, nil)
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			jq.Submit([]byte("hello"), func(ret []byte) {
				if !bytes.Equal(ret, []byte("world")) {
					t.Error("error")
				}
			}, nil, true)
			wg.Done()
		}()
	}
	wg.Wait()
}
