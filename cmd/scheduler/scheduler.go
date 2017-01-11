package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/satori/go.uuid"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Must specify queue.")
	}
	queue := os.Args[1]

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
		fmt.Printf("Successfully scheduled %s\n", jobId)
	}
}
