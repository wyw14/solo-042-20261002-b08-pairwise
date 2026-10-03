package telemetry

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
)

type Receiver struct {
	registry *probe.Registry
	mu       sync.RWMutex
	received uint64
	rejected uint64
}

func NewReceiver(registry *probe.Registry) *Receiver {
	return &Receiver{registry: registry}
}

func (r *Receiver) Publish(probeID string, value float64, observedAt time.Time) error {
	probeState, ok := r.registry.Get(probeID)
	if !ok {
		r.record(false)
		return errors.New("probe not found")
	}
	return r.PublishSample(probeState.NewSample(value, observedAt))
}

func (r *Receiver) PublishSample(sample model.Sample) error {
	probeState, ok := r.registry.Get(sample.ProbeID)
	if !ok {
		r.record(false)
		return errors.New("probe not found")
	}
	if err := validateSample(sample, probeState); err != nil {
		r.record(false)
		return err
	}
	subscription, ok := r.registry.Subscription(sample.ProbeID)
	if !ok || subscription.Closed() {
		r.record(false)
		return errors.New("probe is not accepting samples")
	}
	if !subscription.Send(sample) {
		r.record(false)
		return errors.New("probe sample could not be queued")
	}
	r.record(true)
	return nil
}

func (r *Receiver) record(accepted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if accepted {
		r.received++
	} else {
		r.rejected++
	}
}

func (r *Receiver) Stats() (received, rejected uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.received, r.rejected
}
