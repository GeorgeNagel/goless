package main

import (
	"fmt"
	"log"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	connPool, err := qconn.NewQPool("localhost", "6380", "test_queue", "test-worker")

	if err != nil {
		log.Fatal(err)
	}
	jobId := "12345"
	queue := "test_queue"
	data := "{\"foo\": true}"
	klass := "Fake::Ruby::Class"
	delay := "30"
	err = connPool.ScheduleJob(queue, jobId, klass, data, delay)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully scheduled %s\n", jobId)
	}
}
