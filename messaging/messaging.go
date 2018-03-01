package messaging

import (
	"encoding/json"

	"github.com/lovoo/goka"
)

var (
	SentStream     goka.Stream = "message_sent"
	ReceivedStream goka.Stream = "message_received"
)

// Message ...
type Message struct {
	From    string
	To      string
	Content string
}

// MessageCodec : used for encoding and decoding a message, to/from JSON
type MessageCodec struct{}

// Encode : encodes a message to JSON
func (c *MessageCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Decode : decodes a message from JSON
func (c *MessageCodec) Decode(data []byte) (interface{}, error) {
	var m Message
	return &m, json.Unmarshal(data, &m)
}
