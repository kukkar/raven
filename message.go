package raven

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

//
// If no Explicit type for a message is given this is used.
//
const DEFAULT_MSG_TYPE = "DEF"

//
// Prepare message based on the specified details.
//
func PrepareMessage(id string, mtype string, data string, shardKey string) Message {

	if mtype == "" {
		mtype = DEFAULT_MSG_TYPE
	}
	if id == "" {
		uid, _ := uuid.NewUUID()
		id = uid.String()
	}
	if shardKey == "" {
		shardKey = id
	}
	return Message{
		Id:       id,
		ShardKey: shardKey,
		Data:     data,
		Type:     mtype,
	}
}

//
// The message that is sent and retrieved.
// @todo: need to check if we can avoid json encoding and decoding.
type Message struct {
	//Possibly Unique Id for the message.
	Id string

	//Type of the message
	Type string

	//Content of the message
	Data string

	//used to decide correct message box for the message.
	ShardKey string

	mtime time.Time
}

// String representation of message.
func (this Message) String() string {
	str, _ := json.Marshal(this)
	return string(str)
}

func (this *Message) toJson() string {
	str, err := json.Marshal(this)
	if err != nil {
		fmt.Println(err.Error())
	}
	return string(str)
}

func (this *Message) fromJson(data string) error {
	err := json.Unmarshal([]byte(data), this)
	return err
}

//Check if its an empty message.
func (this *Message) isEmpty() bool {
	if this.Data == "" {
		return true
	}
	return false
}

func (this *Message) getShardKey() string {
	return strings.ToLower(this.ShardKey)
}
