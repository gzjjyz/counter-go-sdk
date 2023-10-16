package counter

import (
	"gopkg.in/redis.v5"
	"sync"
)

const KeyPrefix = "counter_"

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
	p := &RedisProducer{}

	p.redisClient = redis.NewClient(&redis.Options{
		Addr:     config.Host,
		Password: config.Password,
		DB:       config.DB,
	})
	if err := p.redisClient.Ping().Err(); nil != err {
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
				p.redisClient.RPush(KeyPrefix+data.topic, string(data.buf))
			}
		}
	}()

	return nil
}
