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

func NewRedisPool(hostname string, port string) *redis.Pool {
	addr := fmt.Sprintf("%s:%s", hostname, port)
	return &redis.Pool{
		MaxIdle:     0,
		Wait:        true,
		MaxActive:   1,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

// Do one-time setup of redis node to load in Lua scripts.
// Required once during redis lifecycle before any qless commands may be run.
func SetupQlessLua(redisPool *redis.Pool) (string, error) {
	conn := redisPool.Get()
	defer conn.Close()

	sha, err := loadLuaScript(luaScript, conn)
	return sha, err
}

type Qless struct {
	ScriptSha string
	RedisPool *redis.Pool
}

func (qless *Qless) PopJob(queueName string, workerName string) (*JobMetadata, error) {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Bytes(conn.Do("EVALSHA", qless.ScriptSha, 0, "pop", seconds, queueName, workerName, 1))
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
	var job JobMetadata = JobMetadata{Id: jobId, Data: dataString}
	return &job, err
}

func (qless *Qless) Heartbeat(workerName string, jobMetadata *JobMetadata) (int64, error) {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Int64(conn.Do("EVALSHA", qless.ScriptSha, 0, "heartbeat", seconds, jobMetadata.Id, workerName, jobMetadata.Data))
	return result, err
}

func (qless *Qless) CompleteJob(workerName string, jobMetadata *JobMetadata) (string, error) {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", qless.ScriptSha, 0, "complete", seconds, jobMetadata.Id, workerName, jobMetadata.Queue, jobMetadata.Data))
	return result, err
}

func (qless *Qless) FailJob(workerName string, jobMetadata *JobMetadata, group string, message string) (string, error) {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", qless.ScriptSha, 0, "fail", seconds, jobMetadata.Id, workerName, group, message, jobMetadata.Data))
	return result, err
}

func (qless *Qless) ScheduleJob(queue string, jobId string, klass string, data string, delay string) error {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := conn.Do("EVALSHA", qless.ScriptSha, 0, "put", seconds, "me", queue, jobId, klass, data, delay)
	return err
}

func (qless *Qless) StopJob(jobId string) error {
	conn := qless.RedisPool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := conn.Do("EVALSHA", qless.ScriptSha, 0, "cancel", seconds, jobId)
	return err
}

func loadLuaScript(script string, conn redis.Conn) (string, error) {
	sha, err := redis.String(conn.Do("SCRIPT", "LOAD", script))
	return sha, err
}
