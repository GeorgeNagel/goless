package main

import (
	"fmt"
	"log"
	"os"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	connPool, err := qconn.NewQPool("localhost", "6380", "test_queue", "test-worker")
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) < 2 {
		log.Fatal("Must specify job id.")
	}

	jobId := os.Args[1]

	err = connPool.StopJob(jobId)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully stopped %s\n", jobId)
	}
}
