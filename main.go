package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	// "github.com/kdar/goqless"
)

func main() {
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", "localhost", "6380"))
	if err != nil {
		panic(err)
	}

	// Get job ids in queue
	jobs, err := redis.Strings(conn.Do("ZRANGE", "ql:q:audit_events-work", "0", "100"))
	fmt.Printf("Jobs: %s\n", jobs)

	for _, j := range jobs {
		err = printJob(conn, j)
		if err != nil {
			panic(err)
		}
	}
}

func printJob(conn redis.Conn, jobId string) error {
	reply, err := redis.StringMap(conn.Do("HGETALL", fmt.Sprintf("ql:j:%s", jobId)))
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", reply)

	return nil
}
