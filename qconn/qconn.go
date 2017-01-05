package qconn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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

func (pool *QPool) PopJob() (map[string]interface{}, error) {
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
	return jobMap, err
}

func (connPool *QPool) Heartbeat(jobId string, data string) (int64, error) {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Int64(conn.Do("EVALSHA", connPool.ScriptSha, 0, "heartbeat", seconds, jobId, connPool.WorkerName, data))
	return result, err
}

func (connPool *QPool) CompleteJob(jobId string, data string) (string, error) {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "complete", seconds, jobId, connPool.WorkerName, connPool.Queue, data))
	return result, err
}

func (connPool *QPool) FailJob(jobId string, group string, message string, data string) error {
	conn := connPool.Pool.Get()
	defer conn.Close()

	now := time.Now()
	seconds := now.Unix()
	_, err := redis.String(conn.Do("EVALSHA", connPool.ScriptSha, 0, "fail", seconds, jobId, connPool.WorkerName, group, message, data))
	return err
}

func loadLuaScript(script string, conn redis.Conn) (string, error) {
	sha, err := redis.String(conn.Do("SCRIPT", "LOAD", script))
	return sha, err
}

func readLuaScript() (string, error) {
	bytes, err := ioutil.ReadFile("./qless.lua")
	return string(bytes), err
}
