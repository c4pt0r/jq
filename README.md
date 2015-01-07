jq
==

jq is a simple in-app job queue library in go

`jq.NewJq(queueName, queueManager, workerFunc, options)`

`jq.Submit(bytes, onReturnValueFunc, onErrorFunc, isSync)`

usage:

```

func workerFunc(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
	ret <- []byte("world")
	done <- struct{}{}
}

func TestEnqueue(t *testing.T) {
	jq := NewJq("test_queue", MemQueueManagerFactory(MemQFactory), MockWorkerFunc, nil)
	jq.Submit([]byte("hello"), func(ret []byte) {
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}

```
