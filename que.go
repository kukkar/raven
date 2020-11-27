package raven

import (
	"fmt"
	"strconv"
	"strings"
)

//
// Wrapper to create a messageBox.
//
func createMsgBox(name string, bucket string) MsgBox {
	return MsgBox{name: name, boxId: bucket}
}

//
// A messageBox is a virtual repreasentation of a Queue.
//
type MsgBox struct {
	//Name of the Queue
	name string

	//Bucket to which queue belongs.
	boxId string
}

func (this *MsgBox) GetName() string {
	if this.name == "" {
		return ""
	}
	return this.name + "-{" + strings.ToLower(this.boxId) + "}"
}

func (this *MsgBox) GetRawName() string {
	return this.name
}

func (this *MsgBox) GetBoxId() string {
	return strings.ToLower(this.boxId)
}

//
// Exposed method for creation of new Source.
//
func CreateSource(name string, boxes int) Source {
	if boxes < 1 {
		boxes = 1
	}
	msgBoxes := make([]MsgBox, boxes)
	i := 1
	for i <= boxes {
		q := createMsgBox(name, strconv.Itoa(i))
		msgBoxes[i-1] = q
		i++
	}
	s := Source{
		Name:     name,
		MsgBoxes: msgBoxes,
	}
	return s
}

//
// Specifies the Queue Name from which messages needs to be retrieved.
//
// Note: Source is used while message consumption.
//
type Source struct {
	Name     string
	MsgBoxes []MsgBox
}

func (this *Source) GetName() string {
	return this.Name
}

//
// Exposed method for creation of new Destination.
//
func CreateDestination(name string, boxes int, shardlogic ShardHandler) Destination {

	if boxes < 1 {
		boxes = 1
	}
	msgBoxes := make([]MsgBox, boxes)

	i := 1
	for i <= boxes {
		q := createMsgBox(name, strconv.Itoa(i))
		msgBoxes[i-1] = q
		i++
	}
	//incase no shardlogic is provided use default.
	if shardlogic == nil {
		shardlogic = DefaultShardHandler
	}

	d := Destination{
		Name:       name,
		shardLogic: shardlogic,
		MsgBoxes:   msgBoxes,
	}
	return d
}

//
// Destination specifies the location to which the message needs to be sent.
// A destination can contains multiple messageBoxes. Where each message box has its own receiver.
//
// Note: Destination is used while message publication.
//
type Destination struct {
	Name       string
	MsgBoxes   []MsgBox
	shardLogic func(Message, int) (string, error)
}

//
// Get all the message boxes allocated to this destination.
//
func (this *Destination) GetAllBoxes() ([]MsgBox, error) {
	return this.MsgBoxes, nil
}

//
// Decide correct MsgBox for the message based on the assigned ShardKey.
//
func (this *Destination) GetBox4Msg(m Message) (*MsgBox, error) {
	boxId, err := this.shardLogic(m, len(this.MsgBoxes))
	if err != nil {
		return nil, err
	}
	for _, b := range this.MsgBoxes {
		if b.GetBoxId() == boxId {
			return &b, nil
		}
	}
	return nil, fmt.Errorf("Shard Logic Seems to be Incorrect, Msg Box with Id [%s] does not exists", boxId)
}

func (this *Destination) Validate() error {
	//check if name is not empty
	if this.Name == "" {
		return fmt.Errorf("Destination name cannot be empty")
	}
	if len(this.MsgBoxes) <= 0 {
		return fmt.Errorf("Destination does not have any msgbox")
	}
	if this.shardLogic == nil {
		return fmt.Errorf("Shard Logic for destination is empty")
	}
	return nil
}
