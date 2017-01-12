package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Must specify queue.")
	}
	queue := os.Args[1]

	string_max_jobs := os.Getenv("MAX_JOBS")

	var MAX_JOBS = 2
	if string_max_jobs != "" {
		MAX_JOBS, err := strconv.Atoi(string_max_jobs)

		if err != nil {
			log.Fatalf("Invlid MAX_JOBS count: %s, %v \n", string_max_jobs, err)
		}
	}

	fmt.Printf("Current max jobs: %s \n", MAX_JOBS)

	connPool, err := qconn.NewQPool("localhost", "6380", queue, "test-worker")

	if err != nil {
		log.Fatal(err)
	}

	var numberOfJobsMutex = &qconn.JobCounter{Mutex: &sync.Mutex{}, Count: 0}

	for {
		fmt.Printf("[manager] Number of jobs running: %d\n", numberOfJobsMutex.Read())
		// Get job params
		if numberOfJobsMutex.Read() >= MAX_JOBS {
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
