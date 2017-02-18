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
	if e.GetOrigin() == "MetronAgent" {
		return
	}
	switch e.GetEventType() {
	case events.Envelope_CounterEvent:
		m := e.GetCounterEvent()
		log.Printf("%d COUNTER %s:%s:%s:%s %d/%d\n",
			e.GetTimestamp(), e.GetDeployment(), e.GetJob(), e.GetOrigin(),
			m.GetName(), m.GetDelta(), m.GetTotal())

	case events.Envelope_ValueMetric:
		m := e.GetValueMetric()
		log.Printf("%d SAMPLE %s:%s:%s.%s:%s %f\n",
			e.GetTimestamp(), e.GetDeployment(), e.GetJob(), e.GetOrigin(),
			m.GetName(), m.GetUnit(), m.GetValue())

	case events.Envelope_ContainerMetric:
		m := e.GetContainerMetric()
		log.Printf("%d SAMPLE %s:%s:%s:%s:%s:cpu\n",
			e.GetTimestamp(), e.GetDeployment(), e.GetJob(), e.GetOrigin(),
			m.GetApplicationId(), m.GetInstanceIndex(), m.GetCpuPercentage())
		log.Printf("%d SAMPLE %s:%s:%s:%s:%s:mem\n",
			e.GetTimestamp(), e.GetDeployment(), e.GetJob(), e.GetOrigin(),
			m.GetApplicationId(), m.GetInstanceIndex(), m.GetMemoryBytes())
		log.Printf("%d SAMPLE %s:%s:%s:%s:%s:disk\n",
			e.GetTimestamp(), e.GetDeployment(), e.GetJob(), e.GetOrigin(),
			m.GetApplicationId(), m.GetInstanceIndex(), m.GetDiskBytes())
	}
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
