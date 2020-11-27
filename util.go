//
// A utility library to be used within raven.
//

package raven

import (
	"strconv"

	crc16 "github.com/joaojeronimo/go-crc16"
)

//
//  Execute a func with retry on error.
//
func failSafeExec(f func() error, maxtry int) error {
	if maxtry == 0 {
		maxtry = 1
	}
	var current int
	var success bool
	var err error
	for current < maxtry {
		err = f()
		if err == nil {
			success = true
			break
		}
		current += current
	}
	if current == maxtry && !success {
		//its a failure.
		return err
	}
	return nil
}

//
// A default message sharding logic to be used incase none is provided.
//
func DefaultShardHandler(m Message, boxes int) (string, error) {
	slot := (crc16.Crc16([]byte(m.getShardKey()))) % uint16(boxes)
	box := strconv.Itoa(int(slot + 1))
	return box, nil
}
