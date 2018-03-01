package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ricardo-ch/poc-goka/blocker"
	"github.com/ricardo-ch/poc-goka/collector"
	"github.com/ricardo-ch/poc-goka/filter"
	"github.com/ricardo-ch/poc-goka/translator"
)

var (
	brokers       = []string{"localhost:9092"}
	runFilter     = flag.Bool("filter", false, "run filter processor")
	runCollector  = flag.Bool("collector", false, "run collector processor")
	runBlocker    = flag.Bool("blocker", false, "run blocker processor")
	runTranslator = flag.Bool("translator", false, "run translator processor")
	broker        = flag.String("broker", "localhost:9092", "boostrap Kafka broker")
)

func main() {
	flag.Parse()

	// Run the collector : collects messages from the "ReceivedStream"
	if *runCollector {
		log.Println("starting collector")
		go collector.Run(brokers)
	}
	// Run the filter : filters messages from the "SentStream"
	if *runFilter {
		log.Println("starting filter")
		go filter.Run(brokers)
	}
	// Run the blocker : blocks users from the "block_user" stream
	if *runBlocker {
		log.Println("starting blocker")
		go blocker.Run(brokers)
	}
	// Run the translator : translates words from the sent message
	if *runTranslator {
		log.Println("starting translator")
		go translator.Run(brokers)
	}

	// Wait for SIGINT/SIGTERM
	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)

	select {
	case signal := <-waiter:
		log.Printf("Got interrupted by %v", signal)
	}

}
