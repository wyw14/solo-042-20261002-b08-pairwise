package telemetry

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

type Ingestor struct {
	probes    *probe.Registry
	vessels   *vessel.StateService
	snapshots *SnapshotStore
	mu        sync.RWMutex
	accepted  uint64
	dropped   uint64
	observers []func(model.Sample)
}

func (i *Ingestor) AddObserver(observer func(model.Sample)) error {
	if observer == nil {
		return errors.New("telemetry observer is required")
	}
	i.mu.Lock()
	i.observers = append(i.observers, observer)
	i.mu.Unlock()
	return nil
}

func NewIngestor(probes *probe.Registry, vessels *vessel.StateService, snapshots *SnapshotStore) *Ingestor {
	return &Ingestor{probes: probes, vessels: vessels, snapshots: snapshots}
}

// validateSample performs the joint check of a sample against its registered
// probe: identity, probe type, bound vessel, calibration epoch and binding
// generation must all agree, and the probe must be connected. It must run
// before any vessel state change or observer side effect.
func validateSample(sample model.Sample, probeState model.Probe) error {
	switch {
	case sample.ProbeID != probeState.ID:
		return errors.New("sample probe identity does not match the registered probe")
	case !probeState.Connected:
		return errors.New("sample belongs to a disconnected probe")
	case sample.Kind != probeState.Kind:
		return errors.New("sample kind does not match the registered probe type")
	case sample.VesselID != probeState.VesselID:
		return errors.New("sample vessel does not match the probe binding")
	case sample.CalibrationEpoch != probeState.CalibrationEpoch:
		return errors.New("sample belongs to an obsolete calibration epoch")
	case sample.BindingGeneration != probeState.BindingGeneration:
		return errors.New("sample belongs to an obsolete probe generation")
	default:
		return nil
	}
}

func (i *Ingestor) Ingest(sample model.Sample) error {
	probeState, ok := i.probes.Get(sample.ProbeID)
	if !ok {
		return i.reject(errors.New("sample probe is not registered"))
	}
	if err := validateSample(sample, probeState); err != nil {
		return i.reject(err)
	}
	vesselState, ok := i.vessels.Registry().Get(probeState.VesselID)
	if !ok {
		return i.reject(errors.New("sample vessel is not registered"))
	}
	level, pressure, temperature := vesselState.LevelMM, vesselState.PressureKPa, vesselState.TemperatureC
	switch probeState.Kind {
	case model.ProbeLevel:
		level = sample.Value
	case model.ProbePressure:
		pressure = sample.Value
	case model.ProbeTemperature:
		temperature = sample.Value
	default:
		return i.reject(errors.New("unsupported sample kind"))
	}
	updated, err := i.vessels.ApplyTelemetry(probeState.VesselID, level, pressure, temperature, sample.ObservedAt)
	if err != nil {
		return i.reject(err)
	}
	i.snapshots.Apply(updated)
	i.mu.Lock()
	i.accepted++
	observers := append([]func(model.Sample){}, i.observers...)
	i.mu.Unlock()
	for _, observer := range observers {
		observer(sample)
	}
	return nil
}

// reject records a dropped sample. Every error returned by Ingest passes
// through here so the dropped counter always matches the interface result.
func (i *Ingestor) reject(err error) error {
	i.mu.Lock()
	i.dropped++
	i.mu.Unlock()
	return err
}

func (i *Ingestor) Stats() (accepted, dropped uint64) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.accepted, i.dropped
}

func (i *Ingestor) Snapshot() Snapshot {
	return i.snapshots.StableSnapshot()
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.snapshots.StableSnapshot().UpdatedAt
}
