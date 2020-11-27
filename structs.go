//
// Any Global structs should go within this.
//
package raven

import newrelic "github.com/newrelic/go-agent"

type MessageHandler func(m *Message, txn newrelic.Transaction) error

type ShardHandler func(Message, int) (string, error)
