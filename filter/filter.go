package filter

import (
	"strings"

	"github.com/lovoo/goka"
	"github.com/ricardo-ch/poc-goka/blocker"
	"github.com/ricardo-ch/poc-goka/messaging"
	"github.com/ricardo-ch/poc-goka/translator"
)

var (
	// Filter group
	group goka.Group = "message_filter"
)

// Determines if the sent message should be dropped because the sender is blocked
func shouldDrop(ctx goka.Context) bool {
	v := ctx.Join(blocker.Table)
	return v != nil && v.(*blocker.BlockValue).Blocked
}

// Translates the message's words
func translate(ctx goka.Context, m *messaging.Message) *messaging.Message {
	words := strings.Split(m.Content, " ")
	for i, w := range words {
		// Lookup() returns the value for key w in translator.Table
		if tw := ctx.Lookup(translator.Table, w); tw != nil {
			words[i] = tw.(string)
		}
	}
	return &messaging.Message{
		From:    m.From,
		To:      m.To,
		Content: strings.Join(words, " "),
	}
}

// Filters sent messages before sending them to receivers
func filter(ctx goka.Context, msg interface{}) {
	if shouldDrop(ctx) {
		// Blocked user : the message is dropped
		return
	}

	// Translate the message
	m := translate(ctx, msg.(*messaging.Message))

	// Emits the message to the kafka topic ReceivedStream
	ctx.Emit(messaging.ReceivedStream, m.To, m)
}

// Run : runs the filter
func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(messaging.SentStream, new(messaging.MessageCodec), filter),
		goka.Output(messaging.ReceivedStream, new(messaging.MessageCodec)),
		goka.Join(blocker.Table, new(blocker.BlockValueCodec)),
		goka.Lookup(translator.Table, new(translator.ValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
