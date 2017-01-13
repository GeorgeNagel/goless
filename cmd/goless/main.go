package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	var queue string
	flag.StringVar(&queue, "q", "test", "queue name")
	flag.Parse()
	fmt.Printf("[manager] Using queue %s\n", queue)

	envMaxJobs := os.Getenv("MAX_JOBS")

	var maxJobs = 2
	var err error
	if envMaxJobs != "" {
		maxJobs, err = strconv.Atoi(envMaxJobs)

		if err != nil {
			log.Fatalf("[manager] Invlid MAX_JOBS count: %s, %v\n", envMaxJobs, err)
		}
	}

	fmt.Printf("[manager] Current max jobs: %d\n", maxJobs)

	connPool, err := qconn.NewQPool("localhost", "6380", queue, "test-worker")

	if err != nil {
		log.Fatal(err)
	}

	var numberOfJobsMutex = &qconn.JobCounter{Mutex: &sync.Mutex{}, Count: 0}

	for {
		fmt.Printf("[manager] Number of jobs running: %d\n", numberOfJobsMutex.Read())
		// Get job params
		if numberOfJobsMutex.Read() >= maxJobs {
			fmt.Println("[manager] Running jobs at maximum capacity.")
			time.Sleep(10 * time.Second)
			continue
		}
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
