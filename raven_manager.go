package raven

var _ RavenManager = (*RedisSimple)(nil)
var _ RavenManager = (*RedisCluster)(nil)

//
// An interface to be implemented by all Raven Managers.
// Raven Managers contains the exact logic and implementation details of message delivery
// and retrieval.
//
type RavenManager interface {

	// Tasks to be performed at consumer start.
	PreStartup(r MsgReceiver) error

	// Message to be sent, Destination name
	Send(message Message, destination Destination) error

	// Source from which message is to be received.
	// Q in which message is to be stored for temporary basis.
	Receive(r MsgReceiver) (*Message, error)

	// Mark the supplied message as processed.
	MarkProcessed(message *Message, r MsgReceiver) error

	// Mark the supplied message as failed.
	MarkFailed(message *Message, r MsgReceiver) error

	// Provides graceful shutdown of the receiver.
	KillReceiver(r RavenReceiver) error

	//Reque message.
	RequeMessage(message Message, r MsgReceiver) error

	//Show messages reciding in dead Q
	ShowDeadQ(r MsgReceiver) ([]*Message, error)

	//Get messages residing in DeadQ.
	GetDeadQCount(r MsgReceiver) (int, error)

	//Flush DeadQ
	FlushDeadQ(r MsgReceiver) error

	//Flush All associated queues with a Receiver.
	FlushAll(r MsgReceiver) error

	//Get InFlight Messages.
	InFlightMessages(r MsgReceiver) (int, error)

	//Quit receiving messages
	Quit(r MsgReceiver) error
}
