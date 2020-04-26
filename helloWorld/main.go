package main

import (
	"fmt"
	"reflect"

	"github.com/gomodule/redigo/redis"
)

func main() {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()

	reply, _ := c.Do("Ping")
	print("do ping", reply)

	reply, _ = c.Do("hset", "map", "a", 1)
	print("do hset", reply)

	reply, _ = c.Do("hget", "map", "a")
	print("do hget", reply)

	m, _ := redis.StringMap(c.Do("hgetall", "map"))
	print("do hgetall", m)

	reply, _ = c.Do("set", "hello", nil)
	reply, _ = c.Do("get", "hello")
	print("do set", reply)
}

func print(name string, reply interface{}) {
	fmt.Println(name)
	fmt.Println(reflect.TypeOf(reply))
	fmt.Println(reflect.ValueOf(reply))
	fmt.Println(reply)
	fmt.Println()
}
