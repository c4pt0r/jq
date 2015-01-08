jq
==

jq is a simple job queue library in Go.

you can write your own backend queue & queue manager implementations to make jq a real distributed job queue :)

`jq.NewJq(queueName, queueManager, workerFunc)`

`jq.Submit(bytes, onReturnValueFunc, onErrorFunc, isSync)`

usage:

```

func workerFunc(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
	ret <- []byte("world")
	ret <- []byte("world")
	ret <- []byte("world")
	done <- struct{}{}
}

func TestEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MemQueueManagerFactory(MemQFactory), MockWorkerFunc)
	go jq.DispatchForever()
	jq.Submit([]byte("hello"), func(ret []byte) {
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}

```
