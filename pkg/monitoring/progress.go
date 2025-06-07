package monitoring

import (
	"fmt"
	"net/http"

	"github.com/ChaosHour/go-data-checksum/pkg/tracking"
)

type ProgressServer struct {
	Tracker *tracking.JobTracker
	Port    int
}

func (ps *ProgressServer) Start() {
	http.HandleFunc("/progress", ps.handleProgress)
	http.HandleFunc("/differences", ps.handleDifferences)

	fmt.Printf("Progress server starting on port %d\n", ps.Port)
	http.ListenAndServe(fmt.Sprintf(":%d", ps.Port), nil)
}

func (ps *ProgressServer) handleProgress(w http.ResponseWriter, r *http.Request) {
	// Return current job progress as JSON
	// Query tracking database and return status
}

func (ps *ProgressServer) handleDifferences(w http.ResponseWriter, r *http.Request) {
	// Return detailed differences for investigation
}
