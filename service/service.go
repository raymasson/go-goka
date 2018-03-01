package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/ricardo-ch/poc-goka/collector"
	"github.com/ricardo-ch/poc-goka/messaging"
)

// Run : runs the view and the api router
func Run(brokers []string, stream goka.Stream) {
	// Define the view
	view, err := goka.NewView(brokers, collector.Table, new(collector.MessageListCodec))
	if err != nil {
		panic(err)
	}
	// Start the view
	go view.Start()
	// Stop the view
	defer view.Stop()

	// Define the emitter
	emitter, err := goka.NewEmitter(brokers, stream, new(messaging.MessageCodec))
	if err != nil {
		panic(err)
	}
	// Finish the emitter
	defer emitter.Finish()

	// Router
	router := mux.NewRouter()
	router.HandleFunc("/{user}/send", send(emitter, stream)).Methods("POST")
	router.HandleFunc("/{user}/feed", feed(view)).Methods("GET")

	// Start the api
	log.Printf("Listen port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

// Emit a message
func send(emitter *goka.Emitter, stream goka.Stream) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var m messaging.Message

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		err = json.Unmarshal(b, &m)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		m.From = mux.Vars(r)["user"]

		if stream == messaging.ReceivedStream {
			err = emitter.EmitSync(m.To, &m)
		} else {
			err = emitter.EmitSync(m.From, &m)
		}
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}
		log.Printf("Sent message:\n %v\n", m)
		fmt.Fprintf(w, "Sent message:\n %v\n", m)
	}
}

// get the last X messages of a user
func feed(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get the feeder name from the route
		user := mux.Vars(r)["user"]
		// Get the feeder's list of messages
		val, _ := view.Get(user)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", user)
			return
		}
		// Display the feeder's list of messages
		messages := val.([]messaging.Message)
		fmt.Fprintf(w, "Latest messages for %s\n", user)
		for i, m := range messages {
			fmt.Fprintf(w, "%d %10s: %v\n", i, m.From, m.Content)
		}
	}
}
