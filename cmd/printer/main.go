package main

import (
	"fmt"
	"github.com/GeorgeNagel/goless/goless"
	"github.com/GeorgeNagel/goless/qconn"
	"log"
	"time"
)

func main() {
	config := goless.ConfFromEnvAndFlags()
	redisPool := qconn.NewRedisPool(config.RedisHost, config.RedisPort)
	scriptSha, err := qconn.SetupQlessLua(redisPool)
	if err != nil {
		log.Fatal(err)
	}

	qless := qconn.Qless{
		ScriptSha: scriptSha,
		RedisPool: redisPool,
	}

	goless.PollJobQueue(&qless, config, doWork)
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
