package raven

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

//No. of times to try incase of failure.
const MAX_TRY_LIMIT = 3

//Time to wait incase Q is empty.
const BLOCK_FOR_DURATION = 10 * time.Second

var _ RedisClient = (*RedisSimpleClient)(nil)
var _ RedisClient = (*RedisClusterClient)(nil)

type RedisClient interface {
	LPush(string, ...interface{}) *redis.IntCmd
	BRPop(time.Duration, ...string) *redis.StringSliceCmd
	BRPopLPush(string, string, time.Duration) *redis.StringCmd
	RPop(string) *redis.StringCmd
	RPush(key string, values ...interface{}) *redis.IntCmd
	RPopLPush(string, string) *redis.StringCmd
	RPopRPush(string, string) error
	LRange(string, int64, int64) *redis.StringSliceCmd
	Del(keys ...string) *redis.IntCmd
	LLen(key string) *redis.IntCmd
	Close() error
}

type RedisSimpleClient struct {
	*redis.Client
}

func (this *RedisSimpleClient) RPopRPush(popfrom string, pushto string) error {

	return this.Watch(func(tx *redis.Tx) error {
		res := tx.RPop(popfrom)
		data, err := res.Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if err == redis.Nil {
			return ErrEmptyQueue
		}
		pushres := tx.RPush(pushto, data)
		if pushres.Err() != nil {
			return pushres.Err()
		}
		return nil
	}, popfrom)
}

type RedisClusterClient struct {
	*redis.ClusterClient
}

func (this *RedisClusterClient) RPopRPush(popfrom string, pushto string) error {

	err := this.Watch(func(tx *redis.Tx) error {
		res := tx.RPop(popfrom)
		data, err := res.Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if err == redis.Nil {
			return ErrEmptyQueue
		}
		pushres := tx.RPush(pushto, data)
		if pushres.Err() != nil {
			return pushres.Err()
		}
		return nil
	}, popfrom)

	return err
}

//
// A Base client to be implemented by redis and redis cluster.
//
type redisbase struct {
	Client RedisClient
}

//
//  Implementation of Send() method exposed by raven manager.
//
func (this *redisbase) Send(message Message, dest Destination) error {

	box, err := dest.GetBox4Msg(message)
	if err != nil {
		return err
	}

	ret := this.Client.LPush(box.GetName(), message.toJson())
	if ret.Err() != nil {
		return ret.Err()
	}
	return nil
}

func (this *redisbase) Receive(r MsgReceiver) (*Message, error) {

	var message string
	var err error
	if !r.options.isReliable {
		message, err = this.receive(r.msgbox)
	} else {
		message, err = this.receiveReliable(r.msgbox, r.procBox)
	}
	if err != nil {
		return nil, err
	}
	var m *Message = new(Message)
	err = m.fromJson(message)
	return m, nil
}

func (this *redisbase) receive(source MsgBox) (string, error) {
	ret := this.Client.BRPop(BLOCK_FOR_DURATION, source.GetName())
	err := ret.Err()
	if err != nil && err == redis.Nil {
		//we got an error
		return "", ErrEmptyQueue
	}
	if err != nil {
		return "", err
	}
	sliceRes := ret.Val()
	if len(sliceRes) == 2 { //check if its what we expected.
		return sliceRes[1], nil
	}
	return "", fmt.Errorf("An unexpected error occured while fetching message from Q: %s", source)
}

func (this *redisbase) receiveReliable(source MsgBox, procQ MsgBox) (string, error) {
	ret := this.Client.BRPopLPush(source.GetName(), procQ.GetName(), BLOCK_FOR_DURATION)

	err := ret.Err()
	if err != nil && err == redis.Nil {
		//we got an error
		return "", ErrEmptyQueue
	}
	if err != nil {
		return "", err
	}
	sliceRes := ret.Val()
	return sliceRes, nil
}

func (this *redisbase) MarkProcessed(m *Message, r MsgReceiver) error {

	if !r.options.isReliable {
		return nil
	}

	return failSafeExec(func() error { //@todo: use ltrim instead of rpop.
		//to make sure no previous message remains.
		ret := this.Client.RPop(r.procBox.GetName())
		err := ret.Err()
		if err != nil && err != redis.Nil {
			return err
		}
		return nil
	}, MAX_TRY_LIMIT)
}

func (this *redisbase) MarkFailed(m *Message, r MsgReceiver) error {

	if m == nil || (!r.options.isReliable) {
		return nil //nothing to do
	}

	return failSafeExec(func() error {
		ret := this.Client.RPopLPush(r.procBox.GetName(), r.deadBox.GetName())
		err := ret.Err()
		if err != nil && err != redis.Nil {
			return err
		}
		return nil
	}, MAX_TRY_LIMIT)
}

//move any pending items from processingQ to sourceQ.
func (this *redisbase) PreStartup(receiver MsgReceiver) error {
	if !receiver.options.isReliable {
		//no processingQ specified. nothing to do
		return nil
	}
	var finished bool
	//var err error
	for !finished {
		err := this.Client.RPopRPush(receiver.procBox.GetName(), receiver.msgbox.GetName())
		if err == ErrEmptyQueue {
			finished = true
			break
		}
		if err == nil {
			continue
		}
		//something went wrong
		return err
	}
	return nil
}

func (this *redisbase) KillReceiver(r RavenReceiver) error {
	return ErrNotImplemented
}

func (this *redisbase) RequeMessage(message Message, receiver MsgReceiver) error {
	if !receiver.options.isReliable {
		//simply reque message
		ret := this.Client.RPush(receiver.msgbox.GetName(), message.toJson())
		if ret.Err() != nil {
			return ret.Err()
		}
		return nil
	}
	//reque and remove from processing.
	err := this.Client.RPopRPush(receiver.procBox.GetName(), receiver.msgbox.GetName())
	if err == ErrEmptyQueue {
		err = nil
	}
	return err
}

func (this *redisbase) ShowDeadQ(receiver MsgReceiver) ([]*Message, error) {
	res := this.Client.LRange(receiver.deadBox.GetName(), 0, -1)
	err := res.Err()
	if err != nil && err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, _ := res.Result()
	msgs := make([]*Message, 0, len(data))
	for _, v := range data {
		m := new(Message)
		err := m.fromJson(v)
		if err != nil {
			continue
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (this *redisbase) FlushDeadQ(receiver MsgReceiver) error {
	res := this.Client.Del(receiver.deadBox.GetName())
	return res.Err()
}

func (this *redisbase) InFlightMessages(receiver MsgReceiver) (int, error) {
	dat := this.Client.LLen(receiver.msgbox.GetName())
	v, err := dat.Result()
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

func (this *redisbase) GetDeadQCount(r MsgReceiver) (int, error) {
	dat := this.Client.LLen(r.deadBox.GetName())
	v, err := dat.Result()
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

func (this *redisbase) FlushAll(r MsgReceiver) error {
	res := this.Client.Del(r.msgbox.GetName(), r.procBox.GetName(), r.deadBox.GetName())
	return res.Err()
}

func (this *redisbase) Quit(r MsgReceiver) error {
	return this.Client.Close()
}
