package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

func newIngestFixture(t *testing.T, now func() time.Time) (*Ingestor, *vessel.Registry, model.Probe) {
	t.Helper()
	vessels := vessel.NewRegistry()
	vesselOne, err := model.NewVessel("V-01", "Vessel one", "north", now())
	if err != nil {
		t.Fatal(err)
	}
	vesselTwo, err := model.NewVessel("V-02", "Vessel two", "south", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(vesselOne); err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(vesselTwo); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, err := model.NewProbe("P-01", model.ProbePressure, "V-01", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), NewSnapshotStore())
	return ingestor, vessels, probeState
}

func vesselState(t *testing.T, vessels *vessel.Registry, id string) model.Vessel {
	t.Helper()
	state, ok := vessels.Get(id)
	if !ok {
		t.Fatalf("vessel %s not registered", id)
	}
	return state
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
	if accepted, dropped := ingestor.Stats(); accepted != 0 || dropped != 1 {
		t.Fatalf("stats = accepted %d, dropped %d; want 0 accepted, 1 dropped", accepted, dropped)
	}
}

func TestIngestRejectsSpoofedKindAndVessel(t *testing.T) {
	now := time.Now
	ingestor, vessels, probeState := newIngestFixture(t, now)

	observed := 0
	if err := ingestor.AddObserver(func(model.Sample) { observed++ }); err != nil {
		t.Fatal(err)
	}

	// Valid probe ID and current epochs/generation, but the sample claims to be
	// a level probe bound to a different vessel.
	spoofed := probeState.NewSample(120, now())
	spoofed.Kind = model.ProbeLevel
	spoofed.VesselID = "V-02"

	if err := ingestor.Ingest(spoofed); err == nil {
		t.Fatal("spoofed sample was accepted")
	}

	if observed != 0 {
		t.Fatalf("observer invoked %d time(s) for rejected sample", observed)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 0 || dropped != 1 {
		t.Fatalf("stats = accepted %d, dropped %d; want 0 accepted, 1 dropped", accepted, dropped)
	}
	if got := vesselState(t, vessels, "V-01").PressureKPa; got != 0 {
		t.Fatalf("origin vessel pressure changed to %v for rejected sample", got)
	}
	if got := vesselState(t, vessels, "V-02").LevelMM; got != 0 {
		t.Fatalf("spoofed vessel level changed to %v for rejected sample", got)
	}
	if _, ok := ingestor.snapshots.Get("V-02"); ok {
		t.Fatal("rejected sample advanced the snapshot of the spoofed vessel")
	}
	if snapshot := ingestor.Snapshot(); len(snapshot.Vessels) != 0 || !snapshot.UpdatedAt.IsZero() {
		t.Fatalf("snapshot was advanced by rejected sample: %+v", snapshot)
	}
}

func TestIngestRejectsKindMismatch(t *testing.T) {
	now := time.Now
	ingestor, vessels, probeState := newIngestFixture(t, now)

	wrongKind := probeState.NewSample(120, now())
	wrongKind.Kind = model.ProbeLevel // registered probe is a pressure probe

	if err := ingestor.Ingest(wrongKind); err == nil {
		t.Fatal("sample with mismatched kind was accepted")
	}
	if got := vesselState(t, vessels, "V-01").PressureKPa; got != 0 {
		t.Fatalf("vessel pressure changed to %v for kind-mismatched sample", got)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 0 || dropped != 1 {
		t.Fatalf("stats = accepted %d, dropped %d; want 0 accepted, 1 dropped", accepted, dropped)
	}
}

func TestIngestRejectsVesselMismatch(t *testing.T) {
	now := time.Now
	ingestor, vessels, probeState := newIngestFixture(t, now)

	wrongVessel := probeState.NewSample(120, now())
	wrongVessel.VesselID = "V-02" // probe is bound to V-01

	if err := ingestor.Ingest(wrongVessel); err == nil {
		t.Fatal("sample bound to another vessel was accepted")
	}
	if got := vesselState(t, vessels, "V-01").PressureKPa; got != 0 {
		t.Fatalf("bound vessel pressure changed to %v for a foreign-vessel sample", got)
	}
	if got := vesselState(t, vessels, "V-02").PressureKPa; got != 0 {
		t.Fatalf("foreign vessel pressure changed to %v for rejected sample", got)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 0 || dropped != 1 {
		t.Fatalf("stats = accepted %d, dropped %d; want 0 accepted, 1 dropped", accepted, dropped)
	}
}

func TestIngestAcceptsGenuineSample(t *testing.T) {
	now := time.Now
	ingestor, vessels, probeState := newIngestFixture(t, now)

	observed := 0
	if err := ingestor.AddObserver(func(model.Sample) { observed++ }); err != nil {
		t.Fatal(err)
	}

	genuine := probeState.NewSample(120, now())
	if err := ingestor.Ingest(genuine); err != nil {
		t.Fatalf("genuine sample rejected: %v", err)
	}

	if observed != 1 {
		t.Fatalf("observer invoked %d time(s); want 1", observed)
	}
	if got := vesselState(t, vessels, "V-01").PressureKPa; got != 120 {
		t.Fatalf("vessel pressure = %v; want 120", got)
	}
	if got := vesselState(t, vessels, "V-02").PressureKPa; got != 0 {
		t.Fatalf("other vessel pressure = %v; want 0", got)
	}
	if accepted, dropped := ingestor.Stats(); accepted != 1 || dropped != 0 {
		t.Fatalf("stats = accepted %d, dropped %d; want 1 accepted, 0 dropped", accepted, dropped)
	}
}
