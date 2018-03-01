package collector

import (
	"encoding/json"

	"github.com/lovoo/goka"
	"github.com/ricardo-ch/poc-goka/messaging"
)

// Max messages to get by user from the View
const maxMessages = 5

var (
	// Collector group
	group goka.Group = "collector"
	// Table : collector's table
	Table = goka.GroupTable(group)
)

// MessageListCodec : used for encoding and decoding list of messages, to/from JSON
type MessageListCodec struct{}

// Encode : encodes a list of messages to JSON
func (c *MessageListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Decode : decodes a list of messages from JSON
func (c *MessageListCodec) Decode(data []byte) (interface{}, error) {
	var m []messaging.Message
	err := json.Unmarshal(data, &m)
	return m, err
}

// Collect : collect callback is called for every message from ReceivedStream.
// ctx allows access to collector table and msg is the input message.
// ctx is scoped with the key of the input message : the receiver name in our case.
func collect(ctx goka.Context, msg interface{}) {
	var ml []messaging.Message
	// Get the receiver's list of messages
	if v := ctx.Value(); v != nil {
		ml = v.([]messaging.Message)
	}

	// Add the new received message to the list
	m := msg.(*messaging.Message)
	ml = append(ml, *m)

	// Remove the oldest message if the list of messages reached its max size
	if len(ml) > maxMessages {
		ml = ml[len(ml)-maxMessages:]
	}

	// Update the receiver's list of messages in the collector table
	ctx.SetValue(ml)
}

// Run : runs the collector
func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(messaging.ReceivedStream, new(messaging.MessageCodec), collect),
		goka.Persist(new(MessageListCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
