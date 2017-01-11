package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

	var numberOfJobsMutex = &qconn.JobCounter{Mutex: &sync.Mutex{}, Count: 0}

	for {
		fmt.Printf("[manager] Number of jobs running: %d\n", numberOfJobsMutex.Read())
		// Get job params
		job, err := connPool.PopJob()
		if err != nil {
			log.Fatal(err)
		}
		if job == nil {
			fmt.Printf("[manager] Job queue %s empty\n", connPool.Queue)
			time.Sleep(10 * time.Second)
			continue
		}
		fmt.Printf("[manager] About to run: %s\n", job)

		go job.Run(connPool, numberOfJobsMutex)
	}
}
