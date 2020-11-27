package raven

import (
	"github.com/go-redis/redis"
)

type RedisSimple struct {
	redisbase
}

//
// Configuration to Initialize redis cluster.
//
type RedisSimpleConfig struct {
	Addr     string
	Password string
	PoolSize int
}

func InitializeRedis(config RedisSimpleConfig) *RedisSimple {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		PoolSize: config.PoolSize,
	})
	redisS := new(RedisSimple)
	redisS.Client = &RedisSimpleClient{client}
	return redisS
}
