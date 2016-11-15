package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
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

	result, err := popJob(conn, "update_google_accounts", "test-worker", sha)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)

	// // Get job ids in queue
	// jobs, err := redis.Strings(conn.Do("ZRANGE", "ql:q:audit_events-work", "0", "100"))
	// fmt.Printf("Jobs: %s\n", jobs)

	// for _, j := range jobs {
	// 	err = printJob(conn, j)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

}

func popJob(conn redis.Conn, queue string, worker string, scriptSha string) (string, error) {
	result, err := redis.String(conn.Do("EVALSHA", scriptSha, "pop", queue, worker, 1))
	return result, err
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
