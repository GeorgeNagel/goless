package qconn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

func newRedisPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     0,
		Wait:        true,
		MaxActive:   1,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

type QPool struct {
	Pool       *redis.Pool
	Queue      string
	ScriptSha  string
	WorkerName string
}

type Job struct {
	id   string
	data string
}

func (job *Job) Run(connPool *QPool) {
	stopHeartbeat := make(chan string)
	go job.Heartbeat(connPool, 5, stopHeartbeat)

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

func NewQPool(hostname string, port string, queue string, workerName string) (*QPool, error) {
	pool := newRedisPool(fmt.Sprintf("%s:%s", hostname, port))
	conn := pool.Get()
	defer conn.Close()

	script, err := readLuaScript()
	if err != nil {
		return nil, err
	}
	sha, err := loadLuaScript(script, conn)
	if err != nil {
		return nil, err
	}

	return &QPool{Pool: pool, Queue: queue, ScriptSha: sha, WorkerName: workerName}, nil
}

func (pool *QPool) PopJob() (*Job, error) {
	conn := pool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Bytes(conn.Do("EVALSHA", pool.ScriptSha, 0, "pop", seconds, pool.Queue, pool.WorkerName, 1))
	if bytes.Compare(result, []byte("{}")) == 0 {
		// No jobs on the queue
		return nil, nil
	}

	var jobs []interface{}
	err = json.Unmarshal(result, &jobs)
	if err != nil {
		return nil, err
	}

	jobMap, ok := jobs[0].(map[string]interface{})
	if !ok {
		err = errors.New("Could not cast to interface")
		return nil, err
	}

	jobId, ok := jobMap["jid"].(string)
	if !ok {
		log.Fatalf("Job ID \"%v\"is not a string!\n", jobMap["jid"])
	}

	data := jobMap["data"]
	dataString, ok := data.(string)
	if !ok {
		log.Fatal("Job data not a string!")
	}
	var job Job = Job{id: jobId, data: dataString}
	return &job, err
}

func (connPool *QPool) Heartbeat(job *Job) (int64, error) {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Int64(conn.Do("EVALSHA", connPool.ScriptSha, 0, "heartbeat", seconds, job.id, connPool.WorkerName, job.data))
	return result, err
}

func (connPool *QPool) CompleteJob(job *Job) (string, error) {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "complete", seconds, job.id, connPool.WorkerName, connPool.Queue, job.data))
	return result, err
}

func (connPool *QPool) FailJob(job *Job, group string, message string) error {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "fail", seconds, job.id, connPool.WorkerName, group, message, job.data))
	return err
}

func (connPool *QPool) ScheduleJob(queue string, jobId string, klass string, data string, delay string) error {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "put", seconds, "me", queue, jobId, klass, data, delay))
	return err
}

func loadLuaScript(script string, conn redis.Conn) (string, error) {
	sha, err := redis.String(conn.Do("SCRIPT", "LOAD", script))
	return sha, err
}

func readLuaScript() (string, error) {
	bytes, err := ioutil.ReadFile("../../qless.lua")
	return string(bytes), err
}
