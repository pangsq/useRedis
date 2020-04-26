package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/exp/rand"
)

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*100)
	for i := 0; i < 5; i++ {
		go worker(ctx, i)
	}
	<-ctx.Done()
}

func worker(ctx context.Context, index int) {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()

	for {

		// leaderElection
		err := func() error {
			// leaderElection
			reply, err := c.Do("SET", "leader", index, "EX", 1, "NX")
			if err != nil {
				return err
			}
			if reply == nil {
				return errors.New("elect failed")
			}
			return nil
		}()
		if err != nil {
			time.Sleep(time.Duration(rand.Intn(100)+50) * time.Millisecond)
			continue
		}
		log(fmt.Sprintf("Worker %d becomes the leader", index))

		workerCtx, cancel := context.WithCancel(ctx)

		// work
		go func() {
			workTime := time.After(time.Second * 3)
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-workTime:
					cancel() // 工作一定时间后主动交出leader身份
					return
				case <-time.After(time.Second * 1):
					log(fmt.Sprintf("Worker %d is the leader", index))
				}
			}
		}()

		// lease renewal
		ticker := time.NewTicker(time.Millisecond * 500)
		for {
			select {
			case <-workerCtx.Done():
				log(fmt.Sprintf("Worker %d is done", index))
				goto nextLoop
			case <-ticker.C:
				c.Do("EXPIRE", "leader", 1)
				leader, err := redis.Int(c.Do("GET", "leader"))
				if leader != index || err != nil {
					cancel()
					goto nextLoop
				}
			}
		}
	nextLoop:
	}

}

func log(str string) {
	fmt.Println(time.Now().String(), " ", str)
}
