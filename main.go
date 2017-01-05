package main

import (
	"fmt"
	"log"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

func Heartbeat(conn *qconn.QConn, jobId string, dataString string, beatPeriod int, jobDone chan string) {
	for {
		time.Sleep(time.Duration(beatPeriod) * time.Second)
		select {
		case _ = <-jobDone:
			// We have received a message that the job is done and we can stop heart-beating
			return
		default:
			fmt.Printf("[%s] Heartbeating\n", jobId)
			_, err := conn.Heartbeat(jobId, dataString)
			if err != nil {
				fmt.Printf("[%s] Bad heart: %s\n", jobId, err)
			}
		}
	}
}

func main() {
	conn, err := qconn.NewQConn("localhost", "6380", "test_queue", "test-worker")
	if err != nil {
		log.Fatal(err)
	}

	for {
		jobMap, err := conn.PopJob()
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

		jobDone := make(chan string)
		go Heartbeat(conn, jobId, dataString, 5, jobDone)

		// pretend to do actual work
		for i := 0; i < 10; i++ {
			time.Sleep(2 * time.Second)
			fmt.Printf("[%s] Doing some work\n", jobId)
		}

		// err = conn.FailJob(jobId, "I am a fail group", "test fail message", dataString)
		// if err != nil {
		// 	log.Fatalf("Unable to fail Job ID %s\n", jobId)
		// }

		result, err := conn.CompleteJob(jobId, dataString)
		if err != nil {
			fmt.Printf("[%s] Bad complete: %s\n", jobId, err)
		}
		fmt.Println(result)
		jobDone <- "Done!"
	}
}
