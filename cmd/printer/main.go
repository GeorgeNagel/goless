package main

import (
	"fmt"
	"github.com/GeorgeNagel/goless/goless"
)

func main() {
	goless.RunWorker(doWork)
}

func doWork(jobData string, heartbeatPhone chan string) (string, error) {
	fmt.Println("YOLO")
	return "success", nil
}
