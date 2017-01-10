package qconn

import "sync"

type JobCounter struct {
	Mutex *sync.Mutex
	Count int
}

func (counter *JobCounter) Read() int {
	counter.Mutex.Lock()
	defer counter.Mutex.Unlock()
	return counter.Count
}

func (counter *JobCounter) Incr() {
	counter.Mutex.Lock()
	defer counter.Mutex.Unlock()
	counter.Count += 1
}

func (counter *JobCounter) Decr() {
	counter.Mutex.Lock()
	defer counter.Mutex.Unlock()
	counter.Count -= 1
}
