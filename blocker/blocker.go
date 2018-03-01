package blocker

import (
	"encoding/json"

	"github.com/lovoo/goka"
)

var (
	// Blocker group
	group goka.Group = "blocker"
	// Table : blocker's table
	Table = goka.GroupTable(group)
	// Stream : Kafka topic
	Stream goka.Stream = "block_user"
)

// BlockEvent ...
type BlockEvent struct {
	Unblock bool
}

// BlockEventCodec : used for encoding and decoding a block event, to/from JSON
type BlockEventCodec struct{}

// Encode : encodes a block event to JSON
func (c *BlockEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Decode : decodes a block event from JSON
func (c *BlockEventCodec) Decode(data []byte) (interface{}, error) {
	var m BlockEvent
	return &m, json.Unmarshal(data, &m)
}

// BlockValue ...
type BlockValue struct {
	Blocked bool
}

// BlockValueCodec : used for encoding and decoding a block value, to/from JSON
type BlockValueCodec struct{}

// Encode : encodes a block value to JSON
func (c *BlockValueCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Decode : decodes a block value from JSON
func (c *BlockValueCodec) Decode(data []byte) (interface{}, error) {
	var m BlockValue
	return &m, json.Unmarshal(data, &m)
}

// Block : block callback is called for every message from Stream.
// ctx allows access to blocker table and msg is the input message.
// ctx is scoped with the key of the input message.
func block(ctx goka.Context, msg interface{}) {
	// Get the current block value of the user
	var s *BlockValue
	if v := ctx.Value(); v == nil {
		s = new(BlockValue)
	} else {
		s = v.(*BlockValue)
	}

	// Update the block value of the user
	if msg.(*BlockEvent).Unblock {
		s.Blocked = false
	} else {
		s.Blocked = true
	}
	ctx.SetValue(s)
}

// Run : runs the blocker
func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(Stream, new(BlockEventCodec), block),
		goka.Persist(new(BlockValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
