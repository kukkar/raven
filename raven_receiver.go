package raven

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/sanksons/gowraps/util"

	"github.com/kukkar/raven/childlock"
)

//
// Initiate a Raven Receiver.
//
func newRavenReceiver(id string, source Source) (*RavenReceiver, error) {
	rr := new(RavenReceiver)
	//Define source and Id for receiver.
	rr.setSource(source).setId("")

	// Generate Message Receivers, for each message box
	msgreceivers := make([]*MsgReceiver, 0, len(source.MsgBoxes))
	for _, box := range source.MsgBoxes {
		m := &MsgReceiver{
			msgbox:  box,
			parent:  rr,
			stopped: make(chan bool),
		}
		// Set Id for msgReceiver.
		m.setId(box.GetName())
		msgreceivers = append(msgreceivers, m)
	}
	rr.msgReceivers = msgreceivers

	return rr, nil
}

type RavenReceiver struct {

	//A Unique Id that distinguishes this Receiver from other receivers.
	id string

	// Source from which receiver will fetch Ravens.
	source Source

	// If a PORT is specified raven will be locked to this port
	// else an ephemeral port is picked.
	port string

	// Receiving options.
	options struct {
		//Specifies if we want to use reliable Q or not
		//@todo: ordering is yet to be implemented.
		isReliable, ordering bool
	}

	//All the child receivers.
	msgReceivers []*MsgReceiver

	// Access to Raven farm and underlying adapters.
	farm *Farm

	//A lock which ensures singleton receiver.
	lock *childlock.Lock
}

//
// Get identifier for the receiver.
//
func (this *RavenReceiver) GetId() string {
	return this.id
}

//internal function to define id of a receiver.
func (this *RavenReceiver) setId(id string) *RavenReceiver {
	// Make sure we are setting source before Id.
	// Since a Source can have only one receiver at a time, it makes perfect sense to allot
	// source name as ID.
	this.id = this.source.GetName()
	return this
}

//
// Get allocated port for the receiver.
//
func (this *RavenReceiver) GetPort() string {
	return this.port
}

//
// Not mandatory, but if specified receiver will use the specified port for
// communications.
//
func (this *RavenReceiver) SetPort(p string) {
	this.port = p
}

//
// Define the source from which receiver need to look for messages.
//
func (this *RavenReceiver) setSource(s Source) *RavenReceiver {
	this.source = s
	return this
}

//
// Markall the allotted message receivers as reliable.
//
func (this *RavenReceiver) MarkReliable() *RavenReceiver {
	this.options.isReliable = true

	for _, msgReceiver := range this.msgReceivers {
		msgReceiver.markReliable()
	}
	return this
}

//
// Stop the running receiver.
//
func (this *RavenReceiver) Stop() {

	defer func() {
		this.unlock()
		fmt.Printf("\nLock released\n")
	}()
	chanx := make(chan bool)
	for _, receiver := range this.msgReceivers {
		fmt.Printf("\nStopping MsgReceiver: %s", receiver.id)

		go func(receiver *MsgReceiver) {
			receiver.stop()
			chanx <- true
		}(receiver)
	}
	//make sure we wait for msgreceivers to stop.
	for _ = range this.msgReceivers {
		<-chanx
	}
}

//
// Start Raven Receiver.
// 1. Validate
// 2. Register Server.
// 3. Acquire lock.
// 4. Start lock refresher.
// 5. Start all message receivers and heartbeat
// 6. Bootup server.
//
func (this *RavenReceiver) Start(f MessageHandler) error {

	if err := this.validate(); err != nil {
		return err
	}
	//Register server, code is written in such a manner that errors related to
	//address are already caught here.
	server, err := GetServer(this)
	if err != nil {
		return err
	}

	//Take lock, this ensures only one receiver is receiving from Q.
	if err := this.lockme(); err != nil {
		return err
	}
	defer this.unlock()

	//Start a refresher so that lock is refreshed at appropriate intervals
	this.startLockRefresher()

	// execute prestart hook of all receivers.
	// once all prestart hooks are successfull start receivers.
	for _, msgreceiver := range this.msgReceivers {
		if err := msgreceiver.preStart(); err != nil {
			return err
		}
	}

	// Start receivers.
	//   Since the start functions of receivers block, we need to start
	//   receivers as seperate goroutines.
	for _, msgreceiver := range this.msgReceivers {
		go msgreceiver.startHeartBeat()
		go msgreceiver.start(f)
	}

	//Once all the receivers are up boot up the server.
	if err := server.Start(); err != nil {
		return err
	}
	return nil
}

// lock to used to ensure multiple receivers to the same source are not running.
func (this *RavenReceiver) lockme() error {
	if this.lock == nil {
		return nil
	}
	//	fmt.Println("lock")
	r := time.Now().Format(time.RFC3339)
	if err := this.lock.Acquire(r); err != nil {
		return err
	}
	return nil
}

// release lock when the receiver goes down.
func (this *RavenReceiver) unlock() error {
	if this.lock == nil {
		return nil
	}
	//fmt.Println("unlock")
	if err := this.lock.Release(); err != nil {
		return err
	}
	//	fmt.Println("unlock done")
	return nil
}

// ensures that the lock does not dies out, till the receiver is running.
func (this *RavenReceiver) startLockRefresher() error {
	if this.lock == nil {
		return nil
	}
	go func() {
		for {
			time.Sleep(CHILD_LOCK_REFRESH_INTERVAL * time.Second)
			func() {
				//fmt.Println("referesh")
				defer util.PanicHandler("Lock Refresh failed")
				if err := this.lock.Refresh(); err != nil {
					fmt.Printf("Lock refresh failed, Error: %s", err.Error())
				}

			}()
		}
	}()
	return nil
}

func (this *RavenReceiver) validate() error {
	// Check if Id, Source and farm are defined.
	// check if atleast one receiver is assigned.

	if this.id == "" {
		return fmt.Errorf("An Id needs to be assigned to Receiver. Make sure its unique within source")
	}
	if this.source.GetName() == "" {
		return fmt.Errorf("Receiver Source cannot be Empty")
	}
	if this.farm == nil {
		return fmt.Errorf("You need to define to which farm this receiver belongs.")
	}
	if len(this.msgReceivers) <= 0 {
		return fmt.Errorf("Atleast one msg Receiver needs to be assigned")
	}
	return nil
}

//
// Get all the ravens still wandering around.
//
func (this *RavenReceiver) GetInFlightRavens() map[string]string {
	holder := make(map[string]string, len(this.msgReceivers))
	for _, r := range this.msgReceivers {
		var val string
		cc, err := r.getInFlightRavens()
		if err != nil {
			val = err.Error()
		} else {
			val = strconv.Itoa(cc)
		}
		holder[r.id] = val
	}
	return holder
}

//
// Get count of messages sitting in dead box.
//
func (this *RavenReceiver) GetDeadBoxCount() map[string]string {
	holder := make(map[string]string, 0)
	for _, r := range this.msgReceivers {
		var val string
		cc, err := r.GetDeadBoxCount()
		if err != nil {
			val = err.Error()
		} else {
			val = strconv.Itoa(cc)
		}
		holder[r.id] = val
	}
	return holder
}

//
// Flush messages sitting in dead box.
//
func (this *RavenReceiver) FlushDeadBox() map[string]string {
	holder := make(map[string]string, 0)
	for _, r := range this.msgReceivers {
		var val string = "OK"
		if err := r.flushDeadBox(); err != nil {
			val = err.Error()
		}
		holder[r.id] = val
	}
	return holder
}

//
// Flush all messages from all boxes.
//
func (this *RavenReceiver) FlushAll() map[string]string {
	holder := make(map[string]string, 0)
	for _, r := range this.msgReceivers {
		var val string = "OK"
		if err := r.flushAll(); err != nil {
			val = err.Error()
		}
		holder[r.id] = val
	}
	return holder
}

//
// List messages from dead box
//
func (this *RavenReceiver) ShowDeadBox() ([]*Message, error) {
	m := make([]*Message, 0)
	for _, r := range this.msgReceivers {

		msgs, err := r.showDeadBox()
		if err != nil {
			return nil, err
		}
		m = append(m, msgs...)
	}
	return m, nil
}

//
// A informational message to be shown while booting up receiver.
//
func (this *RavenReceiver) ShowMessage() {
	fmt.Println("\n\n--------------------------------------------")
	fmt.Printf("MessageReceivers Started:\n")
	w := tabwriter.NewWriter(os.Stdout, 10, 10, 1, ' ', tabwriter.AlignRight)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "ReceiverId \t IsReliable \t ProcBox \t DeadBox \t ")
	fmt.Fprintln(w)
	for _, r := range this.msgReceivers {
		fmt.Fprintf(w, "%s \t %t \t %s \t %s \t", r.id, r.options.isReliable, r.procBox.GetName(), r.deadBox.GetName())
		fmt.Fprintln(w)
	}
	w.Flush()
	fmt.Println("--------------------------------------------")
	fmt.Printf("\nConsumer Communication Port: %s\n", this.GetPort())
	fmt.Println("--------------------------------------------")
	return
}
