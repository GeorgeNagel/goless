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

func (job *Job) Run(connPool *QPool, counter *JobCounter) {
	stopHeartbeat := make(chan string)
	go job.Heartbeat(connPool, 5, stopHeartbeat)
	counter.Incr()
	defer counter.Decr()

	// pretend to do actual work
	for i := 0; i < 10; i++ {
		select {
		case _ = <-stopHeartbeat:
			fmt.Printf("[%s]Heart stopped. Killing job.\n", job.id)
			return
		default:
			fmt.Printf("[%s] Doing a unit of work\n", job.id)
			time.Sleep(2 * time.Second)
		}
	}

	// Finish the Job
	// Stop heartbeater before telling qless server that we're done
	// in order to avoid heartbeating for a completed job
	stopHeartbeat <- "Done!"
	result, err := connPool.CompleteJob(job)
	if err != nil {
		fmt.Printf("[%s] Bad complete: %s\n", job.id, err)
	}
	fmt.Printf("[%s] %s\n", job.id, result)
}

func (job *Job) Heartbeat(connPool *QPool, beatPeriod int, stopHeartbeat chan string) {
	for {
		time.Sleep(time.Duration(beatPeriod) * time.Second)
		select {
		case _ = <-stopHeartbeat:
			// We have received a message that the job is done and we can stop heart-beating
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
				stopHeartbeat <- "Bad heartbeat/Job canceled"
				return
			}
		}
	}
}
