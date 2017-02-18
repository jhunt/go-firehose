package main

import (
	"fmt"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/jhunt/go-firehose"
)

type MyNozzle struct{}

func (MyNozzle) Configure(c firehose.Config) {
	fmt.Printf("Configure() was called!\n")
}

func (MyNozzle) Track(e *events.Envelope) {
	fmt.Printf("received a %s message from %s\n", e.GetEventType(), e.GetOrigin())
}

func (MyNozzle) Flush() error {
	fmt.Printf("Flush() was called!\n")
	return nil
}

func (MyNozzle) SlowConsumer() {
	fmt.Printf("we are not keeping up with the firehose!\n")
}

func main() {
	firehose.Go(MyNozzle{}, "/etc/firehose.yml")
}
