package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/satori/go.uuid"

	"github.com/GeorgeNagel/goless/qconn"
)

func main() {
	var queue string
	flag.StringVar(&queue, "q", "test", "queue name")
	flag.Parse()

	redisPool := qconn.NewRedisPool("localhost", "6380")
	scriptSha, err := qconn.SetupQlessLua(redisPool)
	if err != nil {
		log.Fatal(err)
	}

	qless := qconn.Qless{
		ScriptSha: scriptSha,
		RedisPool: redisPool,
	}

	uuid := uuid.NewV4().String()
	jobId := strings.Replace(uuid, "-", "", -1)

	data := "{\"foo\": true}"
	klass := "Fake::Ruby::Class"
	delay := "0"

	err = qless.ScheduleJob(queue, jobId, klass, data, delay)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Successfully scheduled %s to queue %s\n", jobId, queue)
	}
}
