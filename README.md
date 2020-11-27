![](raven.png)
## Raven

Does what Ravens are meant to do i.e deliver messages from one place to another.

Supports following engines:
- Redis
- Redis Cluster

## How to Install:

Simply run, below command
```
go get -u github.com/sanksons/raven
```

## How to use:

Detailed examples are kept in examples directory. But for quick view:

### Sending Messages:

Initialize Raven Farm.

```go
//
// Initialize raven farm.
//
farm, _ := raven.InitializeFarm(
    raven.FARM_TYPE_REDISCLUSTER,
    raven.RedisClusterConfig{
        Addrs:    []string{"172.17.0.2:30001"},
        PoolSize: 10,
    },
    nil,
)
```

Pick a raven from Farm.

```go
//Pick a raven from Farm
myraven := farm.GetRaven()
```

Hand over message to raven.

```go
//Hand over message to raven.
myraven.HandMessage(
    raven.PrepareMessage("msgID", "msgType","Message data!!"),
)
```

Specify destinationn for your raven.

```go
const DESTINATION = "product1"
const BUCKET = "1"
//Specify destinationn for your raven.
myraven.SetDestination(raven.CreateDestination(DESTINATION, BUCKET))
```

Make the Raven fly.

```go
//Make the Raven fly.
myraven.Fly()
```

### Receiving Messages:

Initialize Raven farm

```go
//Initialize Raven farm.
farm, _ := raven.InitializeFarm(raven.FARM_TYPE_REDISCLUSTER, raven.RedisClusterConfig{
        Addrs:    []string{"172.17.0.2:30001"},
        PoolSize: 10,
    },
nil,
)
```
Define a source from which to receive.

```go
// Define a source from which to receive.
var source raven.Source = raven.CreateSource(SOURCE, BUCKET)

```

Initiate Receiver and start receiving.

```go
//Initiate Raven Receiver
receiver, _ := farm.GetRavenReceiver("one", source)

//start receiving
err := receiver.Start(func (message *raven.Message) error {
  fmt.Printf("Got message: %s\n", message)
})
```
### Tracking Messages:

How do I track messages ?

You can create you own implementation for "raven.Logger" interface.
or 
Can use one of the built in (raven.Fmtlogger). 

```go
//using fmt logger
loggerStrict := new(raven.FmtLogger)
farm, err := raven.InitializeFarm(raven.FARM_TYPE_REDISCLUSTER, raven.RedisClusterConfig{
        Addrs:    []string{"172.31.#.#:30001"},
        PoolSize: 10,
    },
loggerStrict,
)

```

### Reliability:

We understand that though Ravens are reliable, they can die and we may loose the message.
The reliablility feature safeguards us from this.

```go
// Any receiver can be made reliable by calling.
receiver.MarkReliable()
//Make sure to call it before starting receiver.
```
