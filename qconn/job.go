package qconn

import (
	"fmt"
	"strings"
	"time"
)

type Job struct {
	id   string
	data string
}

func (job *Job) Run(connPool *QPool, counter *JobCounter, fnToRun func(string, chan string) (string, error)) {
	heartbeatPhone := make(chan string)
	defer close(heartbeatPhone)

	go job.Heartbeat(connPool, 5, heartbeatPhone)
	counter.Incr()
	defer counter.Decr()

	status, err := fnToRun(job.data, heartbeatPhone)
	if err != nil {
		fmt.Println("[%s] ERROR: %s", job.id, err.Error)
	}

	if status == "success" {
		// Finish the Job
		// Stop heartbeater before telling qless server that we're done
		// in order to avoid heartbeating for a completed job
		result, err := connPool.CompleteJob(job)
		if err != nil {
			fmt.Printf("[%s] Bad complete: %s\n", job.id, err)
		}
		fmt.Printf("[%s] %s\n", job.id, result)
	} else {
		result, err := connPool.FailJob(job, "failed test jobs", "test-fail-message")
		if err != nil {
			fmt.Printf("[%s] Bad failed: %s\n", job.id, err)
		}
		fmt.Printf("[%s] %s\n", job.id, result)
	}
}

func (job *Job) Heartbeat(connPool *QPool, beatPeriod int, heartbeatPhone chan string) {
	for {
		time.Sleep(time.Duration(beatPeriod) * time.Second)

		// Channel closure is the job telling us to stop heart-beating,
		// either through job completion or a panic.
		select {
		case <-heartbeatPhone:
			// If we got here, the channel was closed
			// The job has told us to stop
			return
		default:
			fmt.Printf("[%s] Heartbeating\n", job.id)
			_, err := connPool.Heartbeat(job)
			if err != nil {
				errMessage := err.Error()

				// "Job does not exist" corresponds to a job canceled in Qless,
				// which qless returns as an error to the heartbeat check-in
				if !strings.Contains(errMessage, "Job does not exist") {
					fmt.Printf("[%s] Unexpected heartbeat error: %s\n", job.id, errMessage)
				}
				// This is how we tell the job that heart-beating is having
				// a problem and that the job should stop.
				heartbeatPhone <- "Bad heartbeat/Job canceled"
				return
			}
		}
	}
}
