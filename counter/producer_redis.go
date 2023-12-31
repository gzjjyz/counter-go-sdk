package counter

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
)

const ChannelSize = 100000

type RedisProducer struct {
	redisClient *redis.Client
	ch          chan *Data
	wg          sync.WaitGroup
}

type RedisConfig struct {
	Host     string `json:"host"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

func NewRedisProducer(config RedisConfig) (Producer, error) {
	p := &RedisProducer{
		ch: make(chan *Data, ChannelSize),
	}

	p.redisClient = redis.NewClient(&redis.Options{
		Addr:     config.Host,
		Password: config.Password,
		DB:       config.DB,
	})
	if err := p.redisClient.Ping(context.Background()).Err(); nil != err {
		return nil, err
	}
	return p, p.init()
}

func (p *RedisProducer) Add(topic string, buf []byte) error {
	p.ch <- &Data{
		topic: topic,
		buf:   buf,
	}
	return nil
}

func (p *RedisProducer) Close() error {
	close(p.ch)
	p.wg.Wait()
	return p.redisClient.Close()
}

func (p *RedisProducer) init() error {
	p.wg.Add(1)

	go func() {
		defer func() {
			p.wg.Done()
		}()

		for {
			select {
			case data, ok := <-p.ch:
				if !ok {
					return
				}
				p.redisClient.RPush(context.Background(), data.topic, string(data.buf))
			}
		}
	}()

	return nil
}
