package jq

import (
	"runtime"
	"sync"
	"testing"

	log "github.com/ngaut/logging"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("info")
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
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			job := jq.Submit([]byte("hello"))
		L:
			for {
				select {
				case r := <-job.RetChan:
					log.Info("ret", job.Id, string(r))
				case err := <-job.ErrChan:
					log.Info("err", job.Id, err)
					break L
				case <-job.Done:
					log.Info("done")
					break L
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
