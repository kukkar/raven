//
// Defines various error types to be used.
//
package raven

import (
	"errors"
)

//Empty message
var ErrNoMessage error = errors.New("A Message Needs to be defined")

// No destination specified
var ErrNoDestination error = errors.New("A Destination Needs to be defined")

//Provided destination is invalid
var ErrInvalidDestination error = errors.New("Invalid Destination defined")

// Q is empty
var ErrEmptyQueue error = errors.New("Empty Queue")

// Not Implemented
var ErrNotImplemented error = errors.New("Feature Not Implemented")

//To be used when a temporary error is encountered.
var ErrTmpFailure error = errors.New("Temporary Failure")
