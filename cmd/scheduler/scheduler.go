package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/satori/go.uuid"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	connPool, err := qconn.NewQPool("localhost", "6380", "test_queue", "test-worker")

	if err != nil {
		log.Fatal(err)
	}
	uuid := uuid.NewV4().String()
	jobId := strings.Replace(uuid, "-", "", -1)

	queue := "test_queue"
	data := "{\"foo\": true}"
	klass := "Fake::Ruby::Class"
	delay := "0"
	err = connPool.ScheduleJob(queue, jobId, klass, data, delay)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully scheduled %s\n", jobId)
	}
}
