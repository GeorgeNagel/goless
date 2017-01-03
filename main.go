package main

import (
	"fmt"
	"log"
	"os"
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
			fmt.Println("Heartbeating")
			_, err := conn.Heartbeat(jobId, dataString)
			if err != nil {
				fmt.Printf("Bad heart: %s", err)
			}
		}
	}
}

func main() {
	conn, err := qconn.NewQConn("localhost", "6380", "test_queue", "test-worker")
	if err != nil {
		log.Fatal(err)
	}

	jobMap, err := conn.PopJob()
	if err != nil {
		log.Fatal(err)
	}
	if jobMap == nil {
		fmt.Println("No jobs on the queue")
		os.Exit(0)
	}
	fmt.Println(jobMap)

	klass := jobMap["klass"]
	fmt.Println(klass)

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
		fmt.Println("Doing some work")
	}

	// err = conn.FailJob(jobId, "I am a fail group", "test fail message", dataString)
	// if err != nil {
	// 	log.Fatalf("Unable to fail Job ID %s\n", jobId)
	// }

	result, err := conn.CompleteJob(jobId, dataString)
	if err != nil {
		fmt.Printf("Bad complete: %s", err)
	}
	fmt.Println(result)
	jobDone <- "Done!"

	// We should stop heartbeating by this point
	time.Sleep(200 * time.Second)
}
