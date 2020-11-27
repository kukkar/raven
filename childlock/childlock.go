package childlock

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
)

//Lock is busy
var ERR_LOCK_BUSY error = errors.New("lock busy")

//Not mine lock
var ERR_NOT_MINE_LOCK error = errors.New("not my lock")

//No such lock exists.
var ERR_NOT_EXISTS error = errors.New("lock not exists")

type RedisOptions struct {
	Addres     []string
	MaxRetries int
	PoolSize   int
}

//
// A utility function to instantiate new LockManager.
//
func NewManager(options RedisOptions) *LockManager {
	opt := redis.UniversalOptions{
		Addrs:      options.Addres,
		MaxRetries: options.MaxRetries,
		PoolSize:   options.PoolSize,
	}
	return &LockManager{
		manager: redis.NewUniversalClient(&opt),
	}
}

//
// A Lockmanager.
//
type LockManager struct {
	manager redis.UniversalClient
}

//
// Create a new lock based on the supplied values
// name:    of the lock
// expiry:  duration in seconds
//
func (this *LockManager) NewLock(name string, expiry int) *Lock {
	return &Lock{
		name:    name,
		expire:  time.Duration(expiry) * time.Second,
		manager: this,
	}
}

//
// Get access to raw redis client.
//
func (this *LockManager) GetClient() redis.UniversalClient {
	return this.manager
}

//
// The main Lock construct.
//
type Lock struct {
	name    string
	value   string
	expire  time.Duration
	manager *LockManager
}

//
// Acquire Lock to a resource.
//
func (this *Lock) Acquire(val string) error {
	ok, err := this.manager.GetClient().SetNX(this.name, val, this.expire).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ERR_LOCK_BUSY
	}
	this.value = val
	return nil
}

//
// Refresh lock to reset expiry of lock.
// Incase losk is already expired, attempt is made to regain lock.
//
func (this *Lock) Refresh() error {
	res, err := this.manager.GetClient().Get(this.name).Result()
	if err != nil && err == redis.Nil {
		//incase lock key does not exists, its better to try and acquire lock
		return this.Acquire(this.value)
	}
	if err != nil {
		return err
	}
	if this.value != res {
		return ERR_NOT_MINE_LOCK
	}
	err = this.manager.GetClient().Watch(func(tx *redis.Tx) error {
		ok, err := tx.Expire(this.name, this.expire).Result()
		if err != nil {
			return err
		}
		if !ok {
			return ERR_NOT_MINE_LOCK
		}
		return nil
	}, this.name)
	return nil
}

//
// Release an acquired lock.
//
func (this *Lock) Release() error {
	res, err := this.manager.GetClient().Get(this.name).Result()
	if err != nil {
		return err
	}
	if this.value != res {
		return ERR_NOT_MINE_LOCK
	}
	err = this.manager.GetClient().Watch(func(tx *redis.Tx) error {
		res, err := tx.Del(this.name).Result()
		if err != nil {
			return err
		}
		if res == 0 {
			return ERR_NOT_MINE_LOCK
		}
		return nil
	}, this.name)

	return err
}
