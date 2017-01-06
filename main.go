package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

func Heartbeat(connPool *qconn.QPool, jobId string, dataString string, beatPeriod int, stopHeartbeat chan string) {
	for {
		time.Sleep(time.Duration(beatPeriod) * time.Second)
		select {
		case _ = <-stopHeartbeat:
			// We have received a message that the job is done and we can stop heart-beating
			return
		default:
			fmt.Printf("[%s] Heartbeating\n", jobId)
			_, err := connPool.Heartbeat(jobId, dataString)
			if err != nil {
				errMessage := err.Error()

				// "Job does not exist" corresponds to a job canceled in Qless,
				// which qless returns as an error to the heartbeat check-in
				if !strings.Contains(errMessage, "Job does not exist") {
					fmt.Printf("[%s] Unexpected heartbeat error: %s\n", jobId, errMessage)
				}
				stopHeartbeat <- "Bad heartbeat/Job canceled"
				return
			}
		}
	}
}

func RunJob(connPool *qconn.QPool, jobId string, dataString string) {
	stopHeartbeat := make(chan string)
	go Heartbeat(connPool, jobId, dataString, 5, stopHeartbeat)

	// pretend to do actual work
	for i := 0; i < 10; i++ {
		select {
		case _ = <-stopHeartbeat:
			fmt.Printf("[%s]Heart stopped. Killing job.\n", jobId)
			return
		default:
			fmt.Printf("[%s] Doing a unit of work\n", jobId)
			time.Sleep(2 * time.Second)
		}
	}

	// Finish the Job
	// Stop heartbeater before telling qless server that we're done
	// in order to avoid heartbeating for a completed job
	stopHeartbeat <- "Done!"
	result, err := connPool.CompleteJob(jobId, dataString)
	if err != nil {
		fmt.Printf("[%s] Bad complete: %s\n", jobId, err)
	}
	fmt.Printf("[%s] %s\n", jobId, result)
}

func main() {
	connPool, err := qconn.NewQPool("localhost", "6380", "test_queue", "test-worker")

	if err != nil {
		log.Fatal(err)
	}

	for {
		// Get job params
		jobMap, err := connPool.PopJob()
		if err != nil {
			log.Fatal(err)
		}
		if jobMap == nil {
			fmt.Println("[manager] No jobs on the queue")
			time.Sleep(10 * time.Second)
			continue
		}
		fmt.Printf("[manager] About to run: %s\n", jobMap)

		jobId, ok := jobMap["jid"].(string)
		if !ok {
			log.Fatalf("Job ID \"%v\"is not a string!\n", jobMap["jid"])
		}

		data := jobMap["data"]
		dataString, ok := data.(string)
		if !ok {
			log.Fatal("Job data not a string!")
		}

		go RunJob(connPool, jobId, dataString)
	}
}
