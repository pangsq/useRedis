package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type DB struct {
	vals map[string]string
	lock sync.RWMutex
}

type OptionalString struct {
	value string
	exist bool
}

func NoExistString() OptionalString {
	return OptionalString{
		value: "",
		exist: false,
	}
}

func (db *DB) get(key string) OptionalString {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if value, ok := db.vals[key]; ok {
		return OptionalString{
			value: value,
			exist: true,
		}
	}
	return NoExistString()
}

func (db *DB) set(key, value string) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.vals[key] = value
}

var db *DB

func main() {
	db = &DB{
		vals: make(map[string]string),
		lock: sync.RWMutex{},
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()
	c.Do("DEL", "key1", "key2", "key3")
	for i := 0; i < 5; i++ {
		go worker(ctx, i)
	}
	<-ctx.Done()
}

func getFromRedis(c redis.Conn, key string) (OptionalString, error) {
	reply, err := c.Do("GET", key)
	if err != nil {
		return NoExistString(), err
	}
	if reply == nil {
		return getFromDB(c, key)
	}
	value, _ := redis.String(reply, nil)
	if value == "<nil>" {
		return NoExistString(), nil
	}
	return OptionalString{
		value: value,
		exist: true,
	}, nil

}

func writeToRedis(c redis.Conn, key string, value string) error {
	_, err := c.Do("DEL", key) // 写时删除redis中的缓存，以保证缓存一致
	if err != nil {
		return err
	}
	db.set(key, value)
	return nil
}

func getFromDB(c redis.Conn, key string) (OptionalString, error) {
	// 防击穿，同一时间只有一个客户端访问DB
	reply, err := c.Do("SET", "GET_FROM_DB_"+key, "", "EX", "1", "NX")
	if err != nil {
		return NoExistString(), err
	}
	defer c.Do("DEL", "GET_FROM_DB_"+key)

	if reply == nil {
		// 没要获得访问DB机会的client，等待一段时间后访问redis取值
		timeout := time.After(time.Second * 2)
		var reply interface{}
		var err error
		for {
			reply, err = c.Do("GET", key)
			if err != nil {
				return NoExistString(), err
			}
			if reply != nil {
				goto out
			}
			select {
			case <-timeout:
				goto out
			case <-time.After(time.Millisecond * time.Duration(200)):
			}
		}

	out:
		value, _ := redis.String(reply, nil)
		if value == "<nil>" {
			return NoExistString(), nil
		}
		return OptionalString{
			value: value,
			exist: true,
		}, err
	}

	ostr := db.get(key)
	if ostr.exist {
		c.Do("SET", key, ostr.value) // 防雪崩，设置随机的过期时间
	} else {
		c.Do("SET", key, "<nil>") // 防穿透
	}

	return ostr, nil
}

func worker(ctx context.Context, index int) {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()
	keys := []string{"key1", "key2", "key3"}
	values := []string{"hello", "world"}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			if rand.Intn(10) > 5 {
				key := keys[rand.Intn(3)]
				ostr, err := getFromRedis(c, key)
				if err != nil {
					fmt.Printf("Worker %d encounter error %v\n", index, err.Error())
				}
				if ostr.exist {
					fmt.Printf("Worker %d knowns %s = %s\n", index, key, ostr.value)
				} else {
					fmt.Printf("Worker %d knows %s not exist\n", index, key)
				}
			} else {
				key, value := keys[rand.Intn(2)], values[rand.Intn(2)]
				fmt.Printf("Worker %d set %v = %v\n", index, key, value)
				writeToRedis(c, key, value)
			}
		}
	}
}
