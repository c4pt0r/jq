package jq

import (
	"runtime"
	"sync"
	"testing"

	log "github.com/ngaut/logging"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("warning")
}

func MockWorker(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
	log.Info("on worker", input)
	ret <- []byte("retVal1")
	ret <- []byte("retVal2")
	ret <- []byte("retVal3")
	ret <- []byte("retVal4")
	done <- struct{}{}
}

func TestEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MockWorker)
	wg := sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			jq.Submit([]byte("hello"), func(ret []byte) {
				log.Info("on ret", string(ret))
			}, nil, true)
			wg.Done()
		}()
	}
	wg.Wait()
}
