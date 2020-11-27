package raven

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
)

//
// Get a interactive server for the receiver.
// Start: In order to start the server call (*ReceiverHolder).Start()
// Stop: In order to stop server call (*ReceiverHolder).Shutdown()
//
func GetServer(receiver *RavenReceiver) (*ReceiverHolder, error) {
	//initiate gin
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	//define holder
	receiverHolder := &ReceiverHolder{
		receiver: receiver,
		engine:   r,
	}
	//define routes
	receiverHolder.defineRoutes()
	//get listener
	listener, err := receiverHolder.getListener()
	if err != nil {
		return nil, err
	}
	receiverHolder.listener = listener
	return receiverHolder, nil
}

//
// Base construct for our interactive server.
//
type ReceiverHolder struct {
	receiver *RavenReceiver
	engine   *gin.Engine
	listener net.Listener
}

// Shutdown is Used to shutdown the server.
//
func (this *ReceiverHolder) Shutdown() error {
	fmt.Println("Shutting down Server...please wait.")
	fmt.Println("######################################")
	this.receiver.Stop()
	fmt.Println("######################################")
	return nil
}

//
// Used to start the server.
//
func (this *ReceiverHolder) Start() error {

	fmt.Println("Starting server ...")
	//register shutdown hook
	this.registerShutdownHook()

	s := http.Server{}
	s.Handler = this.engine

	//show a informative message
	this.receiver.ShowMessage()

	//start serving.
	// go func() {
	// 	s.Serve(this.listener)
	// }()
	// this.registerShutdownHook()
	return s.Serve(this.listener)
}

//register shutdown hook, defines the actions that needs to be performed
//before shutting down server.
func (this *ReceiverHolder) registerShutdownHook() {
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	//fine, wait for signal
	go func() {
		<-quit
		fmt.Println("got shutdown call")
		this.Shutdown()
		os.Exit(0)
	}()
}

//define routing logic
func (this *ReceiverHolder) defineRoutes() {

	this.engine.GET("/", this.ping)
	this.engine.GET("/ping", this.ping)
	this.engine.GET("/stats", this.stats)
	this.engine.GET("/showDeadBox", this.showDeadBox)

	//kill receiver/restart
	//show dead messages.
	this.engine.POST("/flushDead", this.flushDeadQ)
	this.engine.POST("/flushAll", this.flushAll)
}

//called to fetch listener.
func (this *ReceiverHolder) getListener() (net.Listener, error) {
	var port string = "0"
	if this.receiver.port != "" {
		port = this.receiver.port
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, fmt.Errorf("%s", err.Error())
	}
	return listener, nil
}

//
// Routing functions sits below.
//

// stats router.
func (this *ReceiverHolder) stats(c *gin.Context) {

	flightData := this.receiver.GetInFlightRavens()

	deadBoxData := this.receiver.GetDeadBoxCount()
	boxes := make([]string, 0)
	for _, box := range this.receiver.msgReceivers {
		boxes = append(boxes, box.id)
	}

	data := gin.H{
		"Queue":      this.receiver.source.GetName(),
		"IsReliable": this.receiver.options.isReliable,
		"Boxes":      boxes,
		"Inflight":   flightData,
		"DeadBox":    deadBoxData,
	}
	c.JSON(200, data)
}

//flushdeadQ router
func (this *ReceiverHolder) flushDeadQ(c *gin.Context) {

	responsedata := this.receiver.FlushDeadBox()
	data := responsedata
	c.JSON(200, data)
}

//flushall router
func (this *ReceiverHolder) flushAll(c *gin.Context) {
	responsedata := this.receiver.FlushAll()
	data := responsedata
	c.JSON(200, data)
}

//ping router
func (this *ReceiverHolder) ping(c *gin.Context) {
	c.JSON(200, "OK")
}

//showdead router
func (this *ReceiverHolder) showDeadBox(c *gin.Context) {
	msgs, err := this.receiver.ShowDeadBox()
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, msgs)
}
