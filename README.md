jq
==

jq is a simple in-app job queue library in go

usage:

```
jq := NewJq("test_queue", func(input []byte, ret chan<- []byte, done chan <-struct{}, err chan<- error) {
    done <- struct{}{}
})


job := jq.Submit([]byte("job data"))


```
