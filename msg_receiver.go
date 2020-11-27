package raven

import (
	"fmt"
	"time"

	"github.com/go-errors/errors"
	newrelic "github.com/newrelic/go-agent"
	"github.com/sanksons/gowraps/util"
)

//
// Raven Receiver / Message collector
//
type MsgReceiver struct {

	// A unique Id assigned to msgReceiver
	// to distinguish it with other receivers.
	id string

	// RavenReceiver to which this MsgReceiver belongs.
	parent *RavenReceiver

	//Source where to look for messages.
	msgbox MsgBox

	//Options define characteristics of a receiver.
	options struct {
		//Specifies if we want to use reliable Q or not
		//@todo: ordering is yet to be implemented.
		isReliable, ordering bool
	}

	//Q to store processing and dead messages.
	// used only when marked reliable.
	procBox MsgBox
	deadBox MsgBox

	// Flags required to handle proper shutdown of msgreceivers.
	stopFlag bool
	stopped  chan bool
}

func (this MsgReceiver) String() string {
	return fmt.Sprintf("id: %s, msgBox: %s , reliable: %v, processingQ: %s, deadQ: %s",
		this.id, this.msgbox.GetName(), this.options.isReliable, this.procBox.GetName(),
		this.deadBox.GetName(),
	)
}

func (this *MsgReceiver) log(ltype string, msg string) {
	switch ltype {
	case "error":
		this.getLogger().Error(this.id, msg)
	case "warning":
		this.getLogger().Warning(this.id, msg)
	case "info":
		this.getLogger().Info(this.id, msg)
	case "trace":
		this.getLogger().Info(this.id, msg)
	default:
		this.getLogger().Info(this.id, msg)
	}

}

//
// setId defines identifier for msgreceiver.
//
func (this *MsgReceiver) setId(id string) *MsgReceiver {
	// Since a message box can have only one receiver, it makes sense to allot
	// msgBox name as Id.
	this.id = this.msgbox.GetName()
	return this
}

//
// getNewrelicTransaction fetches newrelic.Transaction object
//
func (this *MsgReceiver) getNewrelicTransaction() newrelic.Transaction {
	if this.parent.farm.newrelicApp != nil {
		return this.parent.farm.newrelicApp.StartTransaction(this.id, nil, nil)
	}
	return nil
}

//
// endNewrelicTransaction ends existing newrelic transaction
//
func (this *MsgReceiver) endNewrelicTransaction(txn newrelic.Transaction) {
	if txn == nil {
		return
	}
	txn.End()
}

//
// Record heartbeat of consumer.
//
func (this *MsgReceiver) recordHeartBeat(inflightCount int, deadCount int) {

	if this.parent.farm.newrelicApp == nil {
		return
	}
	//Record Heart Beat
	heartbeatName := "RavenHeartBeat"

	this.parent.farm.newrelicApp.RecordCustomEvent(
		heartbeatName, map[string]interface{}{
			"inflightcount": inflightCount,
			"checkedAt":     int(time.Now().Unix()),
			"queue":         this.msgbox.GetRawName(),
			"box":           this.msgbox.GetBoxId(),
			"deadCount":     deadCount,
		},
	)

	this.getLogger().Info(this.msgbox.GetName(), this.id, "HeartBeat",
		fmt.Sprintf("In Flight Ravens: %d", inflightCount),
	)
}

//get the logger object.
func (this *MsgReceiver) getLogger() Logger {
	return this.parent.farm.logger
}

// Mark the Q as reliable.
func (this *MsgReceiver) markReliable() *MsgReceiver {
	this.options.isReliable = true
	this.defineProcessingQ().defineDeadQ()
	return this
}

// Mark the Q as ordered.
func (this *MsgReceiver) markOrdered() *MsgReceiver {
	this.options.ordering = true
	return this
}

// define processingQ
func (this *MsgReceiver) defineProcessingQ() *MsgReceiver {

	qname := fmt.Sprintf("%s-processing", this.msgbox.GetRawName())
	this.procBox = createMsgBox(qname, this.msgbox.GetBoxId())
	return this
}

// define deadQ
func (this *MsgReceiver) defineDeadQ() *MsgReceiver {

	qname := fmt.Sprintf("%s-dead", this.msgbox.GetRawName())
	this.deadBox = createMsgBox(qname, this.msgbox.GetBoxId())
	return this
}

// Get Messages published but not picked for processing.
func (this *MsgReceiver) getInFlightRavens() (int, error) {
	return this.parent.farm.manager.InFlightMessages(*this)
}

//
// Start HeartBeat of Receiver.
//
func (this *MsgReceiver) startHeartBeat() {
	for {
		func() {
			// Incase of panic, restart for loop.
			defer util.PanicHandler(fmt.Sprintf("HeartBeat: %s", this.id))

			// Pulse rate
			time.Sleep(30 * time.Second)

			cc, err := this.getInFlightRavens()
			if err != nil {
				this.getLogger().Error(this.msgbox.GetName(), this.id, "HeartBeat",
					fmt.Sprintf("Error: %s", err.Error()),
				)
				return
			}
			dc, err := this.GetDeadBoxCount()
			if err != nil {
				this.getLogger().Error(this.msgbox.GetName(), this.id, "HeartBeat",
					fmt.Sprintf("Error: %s", err.Error()),
				)
				return
			}
			//Check if we can record health.
			this.recordHeartBeat(cc, dc)
		}()
	}
}

// ANy validations required for msgreceiver goes here.
func (this *MsgReceiver) validate() error {
	//@todo: implement all the necessary validations required for receiver.
	return nil
}

//
// stop shutdown the msgreceiver
//
func (this *MsgReceiver) stop() {
	this.stopFlag = true
	<-this.stopped
	return
}

//
// This Hook is called before starting the receiver, to ensure that receiver
// meets all the pre stated conditions and will not fail to bootup.
//
func (this *MsgReceiver) preStart() error {
	if verr := this.validate(); verr != nil {
		return verr
	}
	//startup con
	receiver := *this
	if err := this.parent.farm.manager.PreStartup(receiver); err != nil {
		return err
	}
	return nil

}

//
// start starts up the message receiver.
//
func (this *MsgReceiver) start(f MessageHandler) {

	this.log("info", fmt.Sprintf("Starting Raven receiver with config, %s", this))
	receiver := *this

	// this blocks
	for {
		if this.stopFlag {
			fmt.Printf("\nStopped MsgReceiver: %s", this.id)
			this.stopped <- true
			return
		}
		//this blocks, so no need for wait on empty Q.
		msg, err := this.parent.farm.manager.Receive(receiver)

		// Case 1: Queue is Empty, simple recheck.
		if err != nil && err == ErrEmptyQueue {
			//Q is empty, Simply recheck.
			this.log("info", "Queue is empty recheck")
			continue
		}

		// Case 2: Something went wrong, May be server is not reachable.
		// Log, Sleep and retry.
		if err != nil {
			//log error
			this.log("error", fmt.Sprintf("Got Error while receiving. Error: %s", err.Error()))
			this.log("info", "Waiting for 5 seconds before retrying.")
			time.Sleep(5 * time.Second)
			continue
		}

		//Case 3: All went well and a Message is retrieved.
		// process message and retry.
		// - If success, MarkAsProcessed.
		// - If failed with TmpErr, Reque for reprocessing.
		// - If failed with Permanent error, store in DeadBox.
		this.log("info", fmt.Sprintf("Received Message: %s", msg))

		//
		// Send Message for processing.
		//
		execerr := this.processMessage(msg, f)

		if execerr == nil { // Mark as Processed.
			if err := this.markProcessed(msg); err != nil {
				this.log("error",
					fmt.Sprintf("Could Not mark message as processed. Error: %s, Message: %s", err.Error(), msg),
				)
			}
		} else if execerr == ErrTmpFailure { // Requeue Message.
			//@todo: need to check if there should be a limit for requing message.
			// else it might stuck in a never ending loop, if client is sending incorrect error.
			this.log("error", fmt.Sprintf("Got temporary error while processing. message [%s], requeing it", msg))
			if err := this.requeueMessage(*msg); err != nil {
				this.log("error",
					fmt.Sprintf("Could Not Reque message. Error: %s, Message: %s", err.Error(), msg),
				)
			}
			//sleep till 3 seconds, before repulling message.
			time.Sleep(3 * time.Second)

		} else { // Store in DeadBox
			// Found a permanent error while processing message.
			this.log("error", fmt.Sprintf(
				"Got permanent error while processing Message: %s, Discarding it, Error: %s", msg, execerr.Error(),
			))
			if err := this.markFailed(msg); err != nil {
				this.log("error", fmt.Sprintf("Could Not mark message as dead. Error: %s, Message : %s", err.Error(), msg))
			}
		}

	}
}

//
// Upon receiving the message its passed on to this method for processing.
//
func (this *MsgReceiver) processMessage(msg *Message, f MessageHandler) error {
	var execerr error
	var txn newrelic.Transaction
	func() {
		// handle any panics occuring from client code.
		defer func() {
			if r := recover(); r != nil {
				emsg := fmt.Sprintf("Panic Occurred !!! Handled Gracefully \n Message: %s, Stack: %s", msg, errors.Wrap(r, 5).ErrorStack())
				execerr = fmt.Errorf(emsg)
			}
			// Check if transaction is started and needs to be wrapped up.
			//this.endNewrelicTransaction(txn)
		}()

		// Send Message for processing.
		// Note: pass newrelic transaction alongside so that client can
		// make use of it and record segments.
		//txn = this.getNewrelicTransaction()
		execerr = f(msg, txn)
	}()
	return execerr
}

//
// Show contents of deadBox.
//
func (this *MsgReceiver) showDeadBox() ([]*Message, error) {
	return this.parent.farm.manager.ShowDeadQ(*this)
}

//
// Get Count of messages residing in dead box.
//
func (this *MsgReceiver) GetDeadBoxCount() (int, error) {
	return this.parent.farm.manager.GetDeadQCount(*this)
}

//
// Flush Dead Box
//
func (this *MsgReceiver) flushDeadBox() error {
	return this.parent.farm.manager.FlushDeadQ(*this)
}

//
// Flush All messages
//
func (this *MsgReceiver) flushAll() error {
	return this.parent.farm.manager.FlushAll(*this)
}

//
// Mark Message as processed.
//
func (this *MsgReceiver) markProcessed(msg *Message) error {
	// this.parent.farm.newrelicApp.RecordCustomEvent(
	// 	"RavenMessage", map[string]interface{}{
	// 		"processedAt": int(time.Now().Unix()),
	// 		"queue":       this.msgbox.GetRawName(),
	// 		"box":         this.msgbox.GetBoxId(),
	// 		"msgId":       msg.Id,
	// 		"type":        msg.Type,
	// 		"data":        msg.Data,
	// 		"status":      "processed",
	// 	},
	// )
	return this.parent.farm.manager.MarkProcessed(msg, *this)
}

//
// Requeue message incase of tmp error.
//
func (this *MsgReceiver) requeueMessage(msg Message) error {
	return this.parent.farm.manager.RequeMessage(msg, *this)
}

//
// Mark message as failed.
//
func (this *MsgReceiver) markFailed(msg *Message) error {

	// this.parent.farm.newrelicApp.RecordCustomEvent(
	// 	"RavenMessage", map[string]interface{}{
	// 		"processedAt": int(time.Now().Unix()),
	// 		"queue":       this.msgbox.GetRawName(),
	// 		"box":         this.msgbox.GetBoxId(),
	// 		"msgId":       msg.Id,
	// 		"type":        msg.Type,
	// 		"data":        msg.Data,
	// 		"status":      "dead",
	// 	},
	// )
	return this.parent.farm.manager.MarkFailed(msg, *this)
}
