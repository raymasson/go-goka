package translator

import (
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	// Translator group
	group goka.Group = "translator"
	// Table : translator's table
	Table = goka.GroupTable(group)
	// Stream : Translator's stream
	Stream goka.Stream = "translate-word"
)

// ValueCodec : used for encoding and decoding a value, to/from JSON
type ValueCodec struct {
	codec.String
}

func translate(ctx goka.Context, msg interface{}) {
	ctx.SetValue(msg.(string))
}

// Run : runs the translator
func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(Stream, new(ValueCodec), translate),
		goka.Persist(new(ValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
