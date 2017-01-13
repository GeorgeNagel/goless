package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/satori/go.uuid"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	var queue string
	flag.StringVar(&queue, "q", "test", "queue name")
	flag.Parse()

	connPool, err := qconn.NewQPool("localhost", "6380", queue, "test-worker")
	if err != nil {
		log.Fatal(err)
	}

	uuid := uuid.NewV4().String()
	jobId := strings.Replace(uuid, "-", "", -1)

	data := "{\"foo\": true}"
	klass := "Fake::Ruby::Class"
	delay := "0"
	err = connPool.ScheduleJob(queue, jobId, klass, data, delay)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully scheduled %s to queue %s\n", jobId, queue)
	}
}
