package jq

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"

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
	jq := NewJq("test_queue", MemQueueManagerFactory(MemQFactory), MockWorkerFunc, nil)
	go jq.DispatchForever()
	jq.Submit([]byte("hello"), func(ret []byte) {
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}

func TestEnqueueWithTimeout(t *testing.T) {
	jq := NewJq("test_queue", MemQueueManagerFactory(MemQFactory), func(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
		time.Sleep(1 * time.Second)
		ret <- []byte("world")
		done <- struct{}{}
	}, nil)
	go jq.DispatchForever()
	jq.SubmitWithTimeout([]byte("hello"), 500*time.Microsecond, nil, func(err error) {
		log.Info(err)
		if err != ErrTimeout {
			t.Error(err)
		}
	}, true)

	jq.Submit([]byte("hello"), func(ret []byte) {
		log.Info(ret)
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, func(err error) {
		t.Error(err)
	}, true)

}

func TestCocurrentEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MemQueueManagerFactory(MemQFactory), MockWorkerFunc, nil)
	go jq.DispatchForever()
	wg := sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
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
