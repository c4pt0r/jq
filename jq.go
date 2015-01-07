package jq

import (
	"encoding/json"
	"errors"
	"time"

	log "github.com/ngaut/logging"
)

type Worker interface {
	Do([]byte) ([]byte, error)
}

type workerBroker struct {
	w       Worker
	jobChan chan Job
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

type Jq struct {
	name       string
	mgr        QueueManager
	workerFunc WorkerFunc
	waiting    chan *Job
}

type WorkerFunc func(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error)

func NewJq(name string, workerFunc WorkerFunc) *Jq {
	mgr := MemQueueManagerFactory(MemQFactory)
	jq := &Jq{
		name:       name,
		mgr:        mgr,
		workerFunc: workerFunc,
		waiting:    make(chan *Job),
	}

	go func() {
		for job := range jq.waiting {
			q, err := mgr.Get(jq.name + "_waiting_jobs")
			if err != nil {
				job.ErrChan <- err
				continue
			}
			if q != nil {
				b, err := json.Marshal(job)
				if err != nil {
					job.ErrChan <- err
					continue
				}
				err = q.Push(b)
				if err != nil {
					job.ErrChan <- err
					continue
				}
			}
		}
	}()

	// dispatch
	go func() {
		q, err := jq.mgr.Get(jq.name + "_waiting_jobs")
		if err != nil {
			panic(err)
		}

		for {
			b, err := q.Pop()
			if err == ErrEmpty {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				log.Error(err)
				log.Warning("error occurred when fetch job, sleep 1s and retry")
				time.Sleep(1000 * time.Millisecond)
				continue
			}
			var job Job
			err = json.Unmarshal(b, &job)
			if err != nil {
				log.Error(err)
				continue
			}

			// TODO: use a pool
			if workerFunc != nil {
				go func(job Job) {
					retChan := make(chan []byte)
					errChan := make(chan error)
					doneChan := make(chan struct{})
					go workerFunc(job.Data, retChan, doneChan, errChan)
					retq, err := jq.mgr.Get(jq.name + "_job_" + job.Id)
					for {
						var msg Msg
						select {
						case b := <-retChan:
							msg.Type = MSG_RET
							msg.Data = b
						case err := <-errChan:
							msg.Type = MSG_ERR
							msg.Data = []byte(err.Error())
						case <-doneChan:
							msg.Type = MSG_DONE
						}
						b, _ = json.Marshal(msg)
						err = retq.Push(b)
						if err != nil {
							log.Error(err)
							break
						}
						// finish
						if msg.Type == MSG_ERR || msg.Type == MSG_DONE {
							break
						}
					}
				}(job)
			}
		}
	}()
	return jq
}

func (jq *Jq) Submit(data []byte) *Job {
	job := &Job{
		Id:      genId(),
		Data:    data,
		Done:    make(chan struct{}, 1),
		RetChan: make(chan []byte, 1),
		ErrChan: make(chan error, 1),
	}

	go func() {
		// create job return channel
		retQueueName := jq.name + "_job_" + job.Id
		q, err := jq.mgr.Get(retQueueName)
		defer jq.mgr.Del(retQueueName)
		if err != nil {
			job.ErrChan <- err
			return
		}
		for {
			b, err := q.Pop()
			if err == ErrEmpty {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				job.ErrChan <- err
				return
			}
			var msg Msg
			err = json.Unmarshal(b, &msg)
			if err != nil {
				job.ErrChan <- err
				return
			}

			switch msg.Type {
			case MSG_RET:
				job.RetChan <- msg.Data
			case MSG_DONE:
				job.Done <- struct{}{}
				return
			case MSG_ERR:
				job.ErrChan <- errors.New(string(msg.Data))
				return
			}
		}
	}()
	jq.waiting <- job
	return job
}
