package firehose

import (
	"crypto/tls"
	"log"
	"time"

	"github.com/cloudfoundry-incubator/uaago"
	"github.com/cloudfoundry/noaa/consumer"
	noaaerrors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
)

type Destination interface {
	Configure(c Config)
	Track(e *events.Envelope)
	Flush() error
	SlowConsumer()
}

type Firehose struct {
	config   *Config
	errs     <-chan error
	messages <-chan *events.Envelope
	consumer *consumer.Consumer
	client   Destination
}

func Go(dest Destination, config string) {
	c, err := ReadConfig(config)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err)
	}

	dest.Configure(*c)
	f := Firehose{
		client: dest,
		config: c,
	}
	f.Start()
}

func (f *Firehose) Start() error {
	log.Printf("starting up...")
	authToken := f.AuthToken()

	f.consumer = consumer.New(
		f.config.TrafficControllerURL,
		&tls.Config{
			InsecureSkipVerify: f.config.UAA.SkipVerify,
		},
		nil,
	)
	f.consumer.SetIdleTimeout(time.Duration(f.config.idleTimeoutSeconds) * time.Second)
	f.messages, f.errs = f.consumer.Firehose(f.config.Subscription, authToken)

	ticker := time.NewTicker(time.Duration(f.config.flushIntervalSeconds) * time.Second)
	for {
		select {
		case <-ticker.C:
			f.flush()

		case envelope := <-f.messages:
			if !f.keepMessage(envelope) {
				continue
			}

			if envelope.GetEventType() == events.Envelope_CounterEvent && envelope.CounterEvent.GetName() == "TruncatingBuffer.DroppedMessages" && envelope.GetOrigin() == "doppler" {
				log.Printf("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle.")
				f.client.SlowConsumer()
			}
			f.client.Track(envelope)

		case err := <-f.errs:
			f.handleError(err)
			log.Printf("shutting down...")
			return err
		}
	}
}

func (f *Firehose) flush() {
	err := f.client.Flush()
	if err != nil {
		log.Fatalf("FATAL ERROR: %s\n\n", err)
	}
}

func (f *Firehose) handleError(err error) {
	if retryErr, ok := err.(noaaerrors.RetryError); ok {
		err = retryErr.Err
	}

	switch closeErr := err.(type) {
	case *websocket.CloseError:
		switch closeErr.Code {
		case websocket.CloseNormalClosure:
			// nothing to do

		case websocket.ClosePolicyViolation:
			log.Printf("Error while reading from the firehose: %v", err)
			log.Printf("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.")
			f.client.SlowConsumer()
		default:
			log.Printf("Error while reading from the firehose: %v", err)
		}
	default:
		log.Printf("Error while reading from the firehose: %v", err)
	}

	log.Printf("Closing connection with traffic controller due to %v", err)
	f.consumer.Close()
	f.flush()
}

func (f *Firehose) keepMessage(envelope *events.Envelope) bool {
	return true
}

func (f *Firehose) AuthToken() string {
	if f.config.UAA.Disabled {
		return ""
	}

	c, err := uaago.NewClient(f.config.UAA.URL)
	if err != nil {
		log.Fatalf("Failed to connect to UAA at %s: %s", f.config.UAA.URL, err)
	}

	token, err := c.GetAuthToken(f.config.UAA.Client, f.config.UAA.Secret, f.config.UAA.SkipVerify)
	if err != nil {
		log.Fatalf("Error fetching OAuth token for %s: %s.", f.config.UAA.Client, err)
	}
	return token
}
