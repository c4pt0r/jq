jq
==

jq is a simple in-app job queue library in go

usage:

```

func workerFunc(input []byte, ret chan<- []byte, done chan<- struct{}, err chan<- error) {
	ret <- []byte("world")
	done <- struct{}{}
}

func TestEnqueue(t *testing.T) {
	jq := NewJq("test_queue", workerFunc, nil)
	jq.Submit([]byte("hello"), func(ret []byte) {
		if !bytes.Equal(ret, []byte("world")) {
			t.Error("error")
		}
	}, nil, true)
}

```
