package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"time"
)

type QConn struct {
	Conn       redis.Conn
	Queue      string
	ScriptSha  string
	WorkerName string
}

func NewQConn(hostname string, port string, queue string, workerName string) (*QConn, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", hostname, port))
	if err != nil {
		return nil, err
	}
	script, err := readLuaScript()
	if err != nil {
		return nil, err
	}
	sha, err := loadLuaScript(script, conn)
	if err != nil {
		return nil, err
	}

	return &QConn{Conn: conn, Queue: queue, ScriptSha: sha, WorkerName: workerName}, nil
}

func main() {

	conn, err := NewQConn("localhost", "6380", "test_queue", "test-worker")
	if err != nil {
		panic(err)
	}

	jobMap, err := conn.PopJob()
	if err != nil {
		panic(err)
	}
	if jobMap == nil {
		fmt.Println("No jobs on the queue")
		return
	}
	fmt.Println(jobMap)

	klass := jobMap["klass"]
	fmt.Println(klass)

	jobId, ok := jobMap["jid"].(string)
	if !ok {
		panic("NO!")
	}

	data := jobMap["data"]
	dataString, ok := data.(string)
	if !ok {
		panic("Data not a string")
	}

	err = conn.FailJob(jobId, "I am a fail group", "test fail message", dataString)
	if err != nil {
		fmt.Println("Failed to fail")
	}

	// result, _ := conn.CompleteJob(jobId, dataString)
	// fmt.Println(result)
}

func (conn *QConn) PopJob() (map[string]interface{}, error) {
	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Bytes(conn.Conn.Do("EVALSHA", conn.ScriptSha, 0, "pop", seconds, conn.Queue, conn.WorkerName, 1))
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

func (conn *QConn) CompleteJob(jobId string, data string) (string, error) {
	fmt.Printf("Completing job: %s", jobId)
	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Conn.Do("EVALSHA", conn.ScriptSha, 0, "complete", seconds, jobId, conn.WorkerName, conn.Queue, data))
	return result, err
}

func (conn *QConn) FailJob(jobId string, group string, message string, data string) error {
	now := time.Now()
	seconds := now.Unix()
	_, err := redis.String(conn.Conn.Do("EVALSHA", conn.ScriptSha, 0, "fail", seconds, jobId, conn.WorkerName, group, message, data))
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
