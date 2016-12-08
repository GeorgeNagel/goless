package main

import (
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

	klass := jobMap["klass"]
	fmt.Println(klass)
	// // Get job ids in queue
	// jobs, err := redis.Strings(conn.Do("ZRANGE", "ql:q:audit_events-work", "0", "100"))
	// fmt.Printf("Jobs: %s\n", jobs)

	// for _, j := range jobs {
	// 	err = printJob(conn, j)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	//EVALSHA 663a5321ee332d15cedc77d5a5d7404e53c9dc6e 0 'pop' 1 'test-queue' 'test-worker' 1

}

func popJob(conn redis.Conn, queue string, worker string, scriptSha string) (map[string]interface{}, error) {
	now := time.Now()
	seconds := now.Unix()
	result, err := redis.Bytes(conn.Do("EVALSHA", scriptSha, 0, "pop", seconds, queue, worker, 1))

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

func loadLuaScript(script string, conn redis.Conn) (string, error) {
	sha, err := redis.String(conn.Do("SCRIPT", "LOAD", script))
	return sha, err
}

func readLuaScript() (string, error) {
	bytes, err := ioutil.ReadFile("./qless.lua")
	return string(bytes), err
}

func printJob(conn redis.Conn, jobId string) error {
	reply, err := redis.StringMap(conn.Do("HGETALL", fmt.Sprintf("ql:j:%s", jobId)))
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", reply)

	return nil
}
