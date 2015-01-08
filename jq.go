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

func NewJq(name string, queueMgr QueueManager, workerFunc WorkerFunc, opt *JqOptions) *Jq {
	if opt == nil {
		opt = &JqOptions{
			QueueCheckInterval: 100 * time.Millisecond,
		}
	}
	jq := &Jq{
		name:       name,
		mgr:        queueMgr,
		opt:        opt,
		workerFunc: workerFunc,
		waiting:    make(chan *Job),
	}

	go jq.enqueueLoop()
	return jq
}

func (jq *Jq) enqueueLoop() {
	for job := range jq.waiting {
		q, err := jq.mgr.GetOrCreate(jq.name + "_waiting_jobs")
		if err != nil {
			panic(err)
		}
		if q != nil {
			b, _ := json.Marshal(job)
			err = q.Push(b)
			if err != nil {
				log.Error(err, "error occurred when pushing job, sleep 1s and retry")
				time.Sleep(1000 * time.Millisecond)
				continue
			}
		}
	}
}

func (jq *Jq) DispatchForever() {
	q, err := jq.mgr.GetOrCreate(jq.name + "_waiting_jobs")
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
		if jq.workerFunc != nil {
			go jq.doJob(job)
		}
	}
}

func (jq *Jq) doJob(job Job) {
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
	go jq.workerFunc(job.Data, retChan, doneChan, errChan)
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
		b, _ := json.Marshal(msg)
		err = retq.Push(b)
		// this queue maybe had destroied
		if err != nil {
			log.Error(err)
			break
		}
		// finish
		if msg.Type == MSG_ERR || msg.Type == MSG_DONE {
			break
		}
	}
}

func (jq *Jq) Submit(data []byte, onRet func([]byte), onErr func(error), sync bool) {
	jq.SubmitWithTimeout(data, 0, onRet, onErr, sync)
}

func (jq *Jq) waitForResponse(job *Job, respQueue Queue) {
	// remove reponse channel when we are not waiting
	defer jq.mgr.Del(respQueue.Name())

	startTime := time.Now()
	timeoutCheck := true
	// check from response channel
	for {
		b, err := respQueue.Pop()
		if err == ErrEmpty {
			time.Sleep(jq.opt.QueueCheckInterval)
			// check timeout
			if timeoutCheck && job.Timeout > 0 && time.Now().Sub(startTime) > job.Timeout {
				job.onErr(ErrTimeout)
				return
			}
			continue
		}
		if err != nil {
			if job.onErr != nil {
				job.onErr(err)
			}
			return
		}

		// get response
		timeoutCheck = false
		// read response value
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
			return
		case MSG_ERR:
			if job.onErr != nil {
				job.onErr(err)
			}
			return
		}
	}
}

func (jq *Jq) SubmitWithTimeout(data []byte, timeout time.Duration, onRet func([]byte), onErr func(error), sync bool) {
	job := &Job{
		Id:      genId(),
		Data:    data,
		Timeout: timeout,
		onRet:   onRet,
		onErr:   onErr,
	}

	// create response channel
	retQueueName := jq.name + "_job_" + job.Id
	q, err := jq.mgr.GetOrCreate(retQueueName)
	if err != nil {
		if job.onErr != nil {
			job.onErr(err)
		}
		return
	}

	// post job
	jq.waiting <- job

	// wait for response
	if sync {
		jq.waitForResponse(job, q)
	} else {
		go jq.waitForResponse(job, q)
	}
}
