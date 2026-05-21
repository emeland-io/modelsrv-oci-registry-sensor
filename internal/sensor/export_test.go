package sensor

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.emeland.io/modelsrv/pkg/client"
	"go.uber.org/zap"
)

// NewTestServer creates a Server with injected scanners and subscribers for testing.
func NewTestServer(scanners []ImageScanner, urls []string, subscribers []*client.ModelSrvClient, log *zap.SugaredLogger) *Server {
	var rs []registryScanner
	for i, sc := range scanners {
		url := "test-registry"
		if i < len(urls) {
			url = urls[i]
		}
		rs = append(rs, registryScanner{url: url, scanner: sc})
	}
	return &Server{
		scanners:        rs,
		subscribers:     subscribers,
		pollInterval:    time.Hour,
		log:             log,
		knownByRegistry: make(map[string]map[uuid.UUID]struct{}),
	}
}

// ScanOnce exposes the internal scan method for testing.
func (s *Server) ScanOnce(ctx context.Context) {
	s.scan(ctx)
}

// KnownCount returns the total number of tracked resource IDs across all registries.
func (s *Server) KnownCount() int {
	total := 0
	for _, ids := range s.knownByRegistry {
		total += len(ids)
	}
	return total
}
