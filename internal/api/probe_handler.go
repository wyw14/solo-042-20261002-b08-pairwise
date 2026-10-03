package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type sampleRequest struct {
	Value float64 `json:"value"`
}

type rebindRequest struct {
	VesselID string `json:"vessel_id"`
}

func (s *Server) listProbes(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Probes.List()})
}

func (s *Server) publishProbeSample(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	var input sampleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	probeState, ok := s.deps.Probes.Get(probeID)
	if !ok {
		writeError(writer, http.StatusNotFound, "probe not found")
		return
	}
	sample := probeState.NewSample(input.Value, s.deps.Now())
	if err := s.deps.Receiver.PublishSample(sample); err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	s.deps.ProbeMonitor.Observe(sample)
	writeJSON(writer, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) rebindProbe(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	var input rebindRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	transfer, err := s.deps.Transfers.TakeOver(probeID, input.VesselID)
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, transfer)
}
