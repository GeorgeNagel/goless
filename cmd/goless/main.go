package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	connPool, err := qconn.NewQPool("localhost", "6380", "test_queue", "test-worker")

	if err != nil {
		log.Fatal(err)
	}

	var numberOfJobsMutex = &qconn.JobCounter{Mutex: &sync.Mutex{}, Count: 0}

	for {
		fmt.Println(numberOfJobsMutex.Read())
		// Get job params
		job, err := connPool.PopJob()
		if err != nil {
			log.Fatal(err)
		}
		if job == nil {
			fmt.Println("[manager] No jobs on the queue")
			time.Sleep(10 * time.Second)
			continue
		}
		fmt.Printf("[manager] About to run: %s\n", job)

		go job.Run(connPool, numberOfJobsMutex)
	}
}
