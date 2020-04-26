package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/exp/rand"
)

func subAndListen(ctx context.Context, channels ...string) {
	c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialReadTimeout(10*time.Second), redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	psc := redis.PubSubConn{Conn: c}
	err = psc.Subscribe(redis.Args{}.AddFlat(channels)...)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for {
			select {
			case <-ticker.C:
				if err := psc.Ping("ping"); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		defer c.Close()
		defer psc.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			switch n := psc.Receive().(type) {
			case redis.Message:
				fmt.Printf("Sub: message (%v) from channel (%v)\n", string(n.Data), n.Channel)
			case redis.Subscription:
				fmt.Printf("Sub: subscription (kind=%v,count=%v) from channel (%v)\n", n.Kind, n.Count, n.Channel)
			default:
				fmt.Printf("%v\n", n)
			}
		}
	}()
}

func publish(ctx context.Context, channels ...string) {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * time.Duration(rand.Intn(1000))):
			c.Do("PUBLISH", channels[rand.Intn(len(channels))], "hello")
		}
	}
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	channels := []string{"c1", "c2", "c3"}
	subAndListen(ctx, channels...)
	go publish(ctx, channels...)
	<-ctx.Done()
}
