package main

import (
	"fmt"
	"github.com/GeorgeNagel/goless/goless"
	"time"
)

func main() {
	goless.RunWorker(doWork)
}

func doWork(jobData string, heartbeatPhone chan string) (string, error) {
	for i := 0; i < 20; i++ {
		select {
		case <-heartbeatPhone:
			fmt.Println("Job canceled stopping work")
			return "canceled", nil
		default:
			fmt.Println("YOLO")
			time.Sleep(1 * time.Second)
		}
	}
	return "success", nil
}
