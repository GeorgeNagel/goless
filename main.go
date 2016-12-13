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

func main() {

	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", "localhost", "6380"))
	if err != nil {
		panic(err)
	}

	script, err := readLuaScript()
	if err != nil {
		panic(err)
	}

	sha, err := loadLuaScript(script, conn)
	if err != nil {
		panic(err)
	}

	fmt.Println(sha)

	jobMap, err := popJob(conn, "test_queue", "test-worker", sha)
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

	err = failJob(conn, jobId, "test-worker", sha, "test fail message", dataString)
	if err != nil {
		fmt.Println("Failed to fail")
	}

	// result, err := completeJob(conn, jobId, "test_queue", "test-worker", sha, dataString)
	// fmt.Println(result)
}

func popJob(conn redis.Conn, queue string, worker string, scriptSha string) (map[string]interface{}, error) {
	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Bytes(conn.Do("EVALSHA", scriptSha, 0, "pop", seconds, queue, worker, 1))
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

func completeJob(conn redis.Conn, jobId string, queue string, worker string, scriptSha string, data string) (string, error) {
	fmt.Printf("Completing job: %s", jobId)
	now := time.Now()
	seconds := now.Unix()
	result, err := redis.String(conn.Do("EVALSHA", scriptSha, 0, "complete", seconds, jobId, worker, queue, data))
	return result, err
}

func failJob(conn redis.Conn, jobId string, worker string, scriptSha string, message string, data string) error {
	now := time.Now()
	seconds := now.Unix()
	group := "test fail group"
	_, err := redis.String(conn.Do("EVALSHA", scriptSha, 0, "fail", seconds, jobId, worker, group, message, data))
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
