package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	var queue string
	var jobId string
	flag.StringVar(&queue, "q", "test", "queue name")
	flag.StringVar(&jobId, "j", "", "job ID")
	flag.Parse()

	if jobId == "" {
		log.Fatal("Must provide a Job ID")
	}

	connPool, err := qconn.NewQPool("localhost", "6380", queue, "test-worker")
	if err != nil {
		log.Fatal(err)
	}

	err = connPool.StopJob(jobId)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully stopped %s on queue %s\n", jobId, queue)
	}
}
