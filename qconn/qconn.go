package qconn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

func NewQPool(hostname string, port string, queue string, workerName string) (*QPool, error) {
	pool := newRedisPool(fmt.Sprintf("%s:%s", hostname, port))
	conn := pool.Get()
	defer conn.Close()

	sha, err := loadLuaScript(luaScript, conn)
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

func (connPool *QPool) FailJob(job *Job, group string, message string) (string, error) {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "fail", seconds, job.id, connPool.WorkerName, group, message, job.data))
	return result, err
}

func (connPool *QPool) ScheduleJob(queue string, jobId string, klass string, data string, delay string) error {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := conn.Do("EVALSHA", connPool.ScriptSha, 0, "put", seconds, "me", queue, jobId, klass, data, delay)
	return err
}

func (connPool *QPool) StopJob(jobId string) error {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := conn.Do("EVALSHA", connPool.ScriptSha, 0, "cancel", seconds, jobId)
	return err
}

func loadLuaScript(script string, conn redis.Conn) (string, error) {
	sha, err := redis.String(conn.Do("SCRIPT", "LOAD", script))
	return sha, err
}
