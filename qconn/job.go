package qconn

import (
	"fmt"
	"strings"
	"time"
)

// Job return codes
const Success = "successful"
const Canceled = "canceled"
const Failed = "failed"

type JobMetadata struct {
	Id    string
	Data  string
	Queue string
}

func RunJob(qless *Qless, workerName string, heartBeatPeriod int, jobMetadata *JobMetadata, counter *JobCounter, fnToRun func(string, chan string) (string, error)) {
	heartbeatPhone := make(chan string)
	defer close(heartbeatPhone)

	go RunHeartbeat(qless, workerName, jobMetadata, heartBeatPeriod, heartbeatPhone)
	counter.Incr()
	defer counter.Decr()

	status, err := fnToRun(jobMetadata.Data, heartbeatPhone)
	if err != nil {
		fmt.Println("[%s] ERROR: %s", jobMetadata.Id, err.Error)
	}

	if status == Success {
		// Finish the Job
		// Stop heartbeater before telling qless server that we're done
		// in order to avoid heartbeating for a completed job
		result, err := qless.CompleteJob(workerName, jobMetadata)
		if err != nil {
			fmt.Printf("[%s] Bad complete: %s\n", jobMetadata.Id, err)
		}
		fmt.Printf("[%s] %s\n", jobMetadata.Id, result)
	} else if status == Canceled {
		// Job received canceled heartbeat
		fmt.Printf("[%s] Canceled\n", jobMetadata.Id)
	} else if status == Failed {
		result, err := qless.FailJob(workerName, jobMetadata, "failed test jobs", "test-fail-message")
		if err != nil {
			fmt.Printf("[%s] Bad failed: %s\n", jobMetadata.Id, err)
		}
		fmt.Printf("[%s] %s\n", jobMetadata.Id, result)
	} else {
		result, err := qless.FailJob(workerName, jobMetadata, "invalid status response", fmt.Sprintf("Status: %s", status))
		if err != nil {
			fmt.Printf("[%s] Bad failed: %s\n", jobMetadata.Id, err)
		}
		fmt.Printf("[%s] %s\n", jobMetadata.Id, result)
	}
}

func RunHeartbeat(qless *Qless, workerName string, jobMetadata *JobMetadata, beatPeriod int, heartbeatPhone chan string) {
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
			fmt.Printf("[%s] Heartbeating\n", jobMetadata.Id)
			_, err := qless.Heartbeat(workerName, jobMetadata)
			if err != nil {
				errMessage := err.Error()

				// "Job does not exist" corresponds to a job canceled in Qless,
				// which qless returns as an error to the heartbeat check-in
				if !strings.Contains(errMessage, "Job does not exist") {
					fmt.Printf("[%s] Unexpected heartbeat error: %s\n", jobMetadata.Id, errMessage)
				}
				// This is how we tell the job that heart-beating is having
				// a problem and that the job should stop.
				heartbeatPhone <- "Bad heartbeat/Job canceled"
				return
			}
		}
	}
}
