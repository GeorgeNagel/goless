package goless

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GeorgeNagel/goless/qconn"
)

type JobFunc func(string, chan string) (string, error)

func PollJobQueue(qless *qconn.Qless, config Config, fnToRun JobFunc) {
	fmt.Printf("[manager] Current max jobs: %d\n", config.MaxJobs)
	var numberOfJobsMutex = &qconn.JobCounter{Mutex: &sync.Mutex{}, Count: 0}

	for {
		fmt.Printf("[manager] Number of jobs running: %d\n", numberOfJobsMutex.Read())
		if numberOfJobsMutex.Read() >= config.MaxJobs {
			fmt.Println("[manager] Running jobs at maximum capacity.")
			time.Sleep(10 * time.Second)
			continue
		}

		// Get job params
		jobMetadata, err := qless.PopJob(config.Queue, config.WorkerName)
		if err != nil {
			log.Fatal(err)
		}

		if jobMetadata == nil {
			fmt.Printf("[manager] Job queue %s empty\n", config.Queue)
			time.Sleep(10 * time.Second)
			continue
		}

		fmt.Printf("[manager] About to run: %s\n", jobMetadata.Id)
		go qconn.RunJob(qless, config.WorkerName, config.HeartbeatPeriod, jobMetadata, numberOfJobsMutex, fnToRun)
	}
}

type Config struct {
	Queue           string
	MaxJobs         int
	HeartbeatPeriod int
	RedisHost       string
	RedisPort       string
	WorkerName      string
}

func ConfFromEnvAndFlags() Config {
	var queue string
	flag.StringVar(&queue, "q", "test", "queue name")
	flag.Parse()
	fmt.Printf("[manager] Using queue %s\n", queue)

	envMaxJobs := os.Getenv("QL_MAX_JOBS")
	envRedisHost := os.Getenv("QL_REDIS_HOST")
	envRedisPort := os.Getenv("QL_REDIS_PORT")
	envWorkerName := os.Getenv("QL_WORKER_NAME")
	envHeartbeatPeriod := os.Getenv("QL_HEARTBEAT_PERIOD")

	var maxJobs = 2
	var err error
	if envMaxJobs != "" {
		maxJobs, err = strconv.Atoi(envMaxJobs)
		if err != nil {
			log.Fatalf("[manager] Invlid MAX_JOBS count: %s, %v\n", envMaxJobs, err)
		}
	}
	var heartbeatPeriod = 10
	if envHeartbeatPeriod != "" {
		heartbeatPeriod, err = strconv.Atoi(envHeartbeatPeriod)
		if err != nil {
			log.Fatalf("[manager] Invlid MAX_JOBS count: %s, %v\n", envMaxJobs, err)
		}
	}
	var redisHost = "localhost"
	if envRedisHost != "" {
		redisHost = envRedisHost
	}
	var redisPort = "6380"
	if envRedisPort != "" {
		redisPort = envRedisPort
	}
	var workerName = "test-worker"
	if envWorkerName != "" {
		workerName = envWorkerName
	}
	return Config{
		Queue:           queue,
		MaxJobs:         maxJobs,
		HeartbeatPeriod: heartbeatPeriod,
		RedisHost:       redisHost,
		RedisPort:       redisPort,
		WorkerName:      workerName,
	}
}
