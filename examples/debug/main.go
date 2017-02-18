package main

import (
	"log"
	"os"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/jhunt/go-firehose"
)

type Nozzle struct{}

func (Nozzle) Configure(c firehose.Config) {
}

func (Nozzle) Track(e *events.Envelope) {
	log.Printf("MESSAGE -------------------------\n")
	log.Printf("     origin %s\n", e.GetOrigin())
	log.Printf("       type %s\n", e.GetEventType())
	log.Printf("  timestamp %d\n", e.GetTimestamp())
	log.Printf(" deployment %s\n", e.GetDeployment())
	log.Printf("        job %s\n", e.GetJob())
	log.Printf("      index %s\n", e.GetIndex())
	log.Printf("         ip %s\n", e.GetIp())
	switch e.GetEventType() {
	case events.Envelope_CounterEvent:
		m := e.GetCounterEvent()
		log.Printf("     metric %s\n", m.GetName())
		log.Printf("      delta %d\n", m.GetDelta())
		log.Printf("      total %d\n", m.GetTotal())

	case events.Envelope_ValueMetric:
		m := e.GetValueMetric()
		log.Printf("     metric %s\n", m.GetName())
		log.Printf("      value %f %s\n", m.GetValue(), m.GetUnit())

	case events.Envelope_ContainerMetric:
		m := e.GetContainerMetric()
		log.Printf("application %s\n", m.GetApplicationId())
		log.Printf("   instance %s\n", m.GetInstanceIndex())
		log.Printf("        cpu %f %%\n", m.GetCpuPercentage())
		log.Printf("     memory %f b\n", m.GetMemoryBytes())
		log.Printf("       disk %f b\n", m.GetDiskBytes())
		log.Printf(" mem. quota %f b\n", m.GetMemoryBytesQuota())
		log.Printf(" disk quota %f b\n", m.GetDiskBytesQuota())
	}
	log.Printf("---------------------------------\n\n")
	return
}

func (Nozzle) Flush() error {
	return nil
}

func (Nozzle) SlowConsumer() {
}

func main() {
	var config string
	if len(os.Args) > 1 {
		config = os.Args[1]
	}
	firehose.Go(Nozzle{}, config)
}
