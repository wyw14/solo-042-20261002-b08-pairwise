package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

func newTestIngestor(t *testing.T, vessels ...model.Vessel) (*Ingestor, *probe.Registry, *vessel.StateService, *SnapshotStore) {
	t.Helper()
	now := time.Now
	registry := vessel.NewRegistry()
	for _, v := range vessels {
		if err := registry.Register(v); err != nil {
			t.Fatal(err)
		}
	}
	states := vessel.NewStateService(registry, now)
	probes := probe.NewRegistry(8)
	snapshots := NewSnapshotStore()
	snapshots.Replace(registry.List(), now())
	return NewIngestor(probes, states, snapshots), probes, states, snapshots
}

func TestIngestRejectsObsoleteBindingGeneration(t *testing.T) {
	now := time.Now
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", now())
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now())
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), NewSnapshotStore())
	stale := probeState.NewSample(50, now())
	if _, err := probe.NewLifecycle(probes, now).Rebind("P-01", "V-02"); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("obsolete binding sample was accepted")
	}
}

// A valid level-probe ID/current generation must not be allowed to claim a
// different type and a different vessel: neither vessel state nor observers may
// be touched, and the dropped counter must move in step with the error result.
func TestIngestRejectsSpoofedKindAndVessel(t *testing.T) {
	now := time.Now()
	v1, _ := model.NewVessel("V-01", "Vessel 01", "north", now)
	v2, _ := model.NewVessel("V-02", "Vessel 02", "south", now)
	ingestor, probes, states, _ := newTestIngestor(t, v1, v2)

	// P-01 is a level probe bound to V-01.
	p01, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now)
	if err := probes.Register(p01); err != nil {
		t.Fatal(err)
	}
	// Seed V-01 with known telemetry so a write would be observable.
	if _, err := states.ApplyTelemetry("V-01", 100, 120, 5, now); err != nil {
		t.Fatal(err)
	}

	observed := 0
	if err := ingestor.AddObserver(func(model.Sample) { observed++ }); err != nil {
		t.Fatal(err)
	}

	// Forged sample: valid identity + current epochs, but claims to be a
	// pressure probe belonging to V-02.
	forged := p01.NewSample(999, now)
	forged.Kind = model.ProbePressure
	forged.VesselID = "V-02"

	if err := ingestor.Ingest(forged); err == nil {
		t.Fatal("spoofed kind/vessel sample was accepted")
	}

	after1, _ := states.Registry().Get("V-01")
	after2, _ := states.Registry().Get("V-02")
	if after1.PressureKPa != 120 {
		t.Fatalf("original vessel pressure was rewritten: got %v want 120", after1.PressureKPa)
	}
	if after1.LevelMM != 100 {
		t.Fatalf("original vessel level changed unexpectedly: %v", after1.LevelMM)
	}
	if after2.PressureKPa != 0 || !after2.UpdatedAt.Equal(v2.UpdatedAt) {
		t.Fatal("the other vessel state was touched by a rejected sample")
	}
	if observed != 0 {
		t.Fatalf("observer fired %d time(s) for a rejected sample", observed)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 0 || dropped != 1 {
		t.Fatalf("stats = (accepted %d, dropped %d), want (0, 1)", accepted, dropped)
	}
}

// A kind mismatch that keeps the same vessel must also be rejected without
// rewriting the wrong field.
func TestIngestRejectsKindMismatchSameVessel(t *testing.T) {
	now := time.Now()
	v1, _ := model.NewVessel("V-01", "Vessel 01", "north", now)
	ingestor, probes, states, _ := newTestIngestor(t, v1)

	p01, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now)
	if err := probes.Register(p01); err != nil {
		t.Fatal(err)
	}
	if _, err := states.ApplyTelemetry("V-01", 100, 120, 5, now); err != nil {
		t.Fatal(err)
	}

	forged := p01.NewSample(999, now)
	forged.Kind = model.ProbePressure // same vessel, wrong type
	if err := ingestor.Ingest(forged); err == nil {
		t.Fatal("kind-mismatched sample was accepted")
	}
	got, _ := states.Registry().Get("V-01")
	if got.PressureKPa != 120 {
		t.Fatalf("pressure rewritten by a level probe: got %v want 120", got.PressureKPa)
	}
}

// Correct, well-formed samples still flow through and update the bound vessel,
// the accepted counter and the observers exactly once.
func TestIngestAcceptsMatchingSample(t *testing.T) {
	now := time.Now()
	v1, _ := model.NewVessel("V-01", "Vessel 01", "north", now)
	ingestor, probes, states, _ := newTestIngestor(t, v1)

	p01, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now)
	if err := probes.Register(p01); err != nil {
		t.Fatal(err)
	}
	observed := 0
	if err := ingestor.AddObserver(func(model.Sample) { observed++ }); err != nil {
		t.Fatal(err)
	}

	sample := p01.NewSample(77, now)
	if err := ingestor.Ingest(sample); err != nil {
		t.Fatalf("valid sample rejected: %v", err)
	}
	got, _ := states.Registry().Get("V-01")
	if got.LevelMM != 77 {
		t.Fatalf("level not updated: got %v want 77", got.LevelMM)
	}
	if observed != 1 {
		t.Fatalf("observer fired %d time(s), want 1", observed)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 1 || dropped != 0 {
		t.Fatalf("stats = (accepted %d, dropped %d), want (1, 0)", accepted, dropped)
	}
}

// The gateway receiver must reject the forged sample before queueing it, and
// its reject counter must agree with the returned error.
func TestReceiverRejectsSpoofedSample(t *testing.T) {
	now := time.Now()
	probes := probe.NewRegistry(4)
	p01, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now)
	if err := probes.Register(p01); err != nil {
		t.Fatal(err)
	}
	receiver := NewReceiver(probes)

	forged := p01.NewSample(50, now)
	forged.Kind = model.ProbePressure
	forged.VesselID = "V-02"

	if err := receiver.PublishSample(forged); err == nil {
		t.Fatal("receiver accepted a spoofed sample")
	}
	sub, ok := probes.Subscription("P-01")
	if !ok {
		t.Fatal("probe subscription missing")
	}
	if queued := len(sub.Samples()); queued != 0 {
		t.Fatalf("forged sample was queued (%d item)", queued)
	}
	if received, rejected := receiver.Stats(); received != 0 || rejected != 1 {
		t.Fatalf("receiver stats = (received %d, rejected %d), want (0, 1)", received, rejected)
	}

	// A valid sample from the same probe still gets through.
	if err := receiver.PublishSample(p01.NewSample(40, now)); err != nil {
		t.Fatalf("valid sample rejected: %v", err)
	}
	if received, rejected := receiver.Stats(); received != 1 || rejected != 1 {
		t.Fatalf("receiver stats = (received %d, rejected %d), want (1, 1)", received, rejected)
	}
}

