package jq

import (
	"encoding/json"
	"time"

	log "github.com/ngaut/logging"
)

type WorkerFunc func(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error)

type Jq struct {
	name       string
	mgr        QueueManager
	workerFunc WorkerFunc
	opt        *JqOptions
	waiting    chan *Job
}

type JqOptions struct {
	QueueCheckInterval time.Duration
}

func NewJq(name string, workerFunc WorkerFunc, opt *JqOptions) *Jq {
	if opt == nil {
		opt = &JqOptions{
			QueueCheckInterval: 100 * time.Millisecond,
		}
	}
	mgr := MemQueueManagerFactory(MemQFactory)
	jq := &Jq{
		name:       name,
		mgr:        mgr,
		opt:        opt,
		workerFunc: workerFunc,
		waiting:    make(chan *Job),
	}

	go func() {
		for job := range jq.waiting {
			q, err := mgr.Get(jq.name + "_waiting_jobs")
			if err != nil {
				continue
			}
			if q != nil {
				b, err := json.Marshal(job)
				if err != nil {
					continue
				}
				err = q.Push(b)
				if err != nil {
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
				time.Sleep(jq.opt.QueueCheckInterval)
				continue
			}
			if err != nil {
				log.Error(err, "error occurred when fetching job, sleep 1s and retry")
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
					retQueueName := jq.name + "_job_" + job.Id
					if b, _ := jq.mgr.Exists(retQueueName); !b {
						log.Warning("return channel not ready, ignore this job")
						return
					}
					retq, err := jq.mgr.Get(retQueueName)
					if err != nil {
						log.Warning("get return channel error, ignore this job")
						return
					}
					retChan := make(chan []byte)
					errChan := make(chan error)
					doneChan := make(chan struct{})
					go workerFunc(job.Data, retChan, doneChan, errChan)
					// wait for worker response
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

func (jq *Jq) Submit(data []byte, onRet func([]byte), onErr func(error), sync bool) {
	job := &Job{
		Id:    genId(),
		Data:  data,
		onRet: onRet,
		onErr: onErr,
	}

	var done chan struct{}
	if sync {
		done = make(chan struct{})
	}

	go func() {
		// create job return channel
		retQueueName := jq.name + "_job_" + job.Id
		q, err := jq.mgr.Get(retQueueName)
		defer jq.mgr.Del(retQueueName)
		if err != nil {
			if job.onErr != nil {
				job.onErr(err)
			}
			return
		}
		for {
			b, err := q.Pop()
			if err == ErrEmpty {
				time.Sleep(jq.opt.QueueCheckInterval)
				continue
			}
			if err != nil {
				if job.onErr != nil {
					job.onErr(err)
				}
				return
			}
			var msg Msg
			err = json.Unmarshal(b, &msg)
			if err != nil {
				if job.onErr != nil {
					job.onErr(err)
				}
				return
			}

			switch msg.Type {
			case MSG_RET:
				if job.onRet != nil {
					job.onRet(msg.Data)
				}
			case MSG_DONE:
				if sync {
					done <- struct{}{}
				}
				return
			case MSG_ERR:
				if job.onErr != nil {
					job.onErr(err)
				}
				return
			}
		}
	}()
	jq.waiting <- job
	if sync {
		<-done
	}
}
