package sensor

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.emeland.io/modelsrv/pkg/client"
	"go.uber.org/zap"
)

// NewTestServer creates a Server with injected scanners and subscribers for testing.
func NewTestServer(scanners []ImageScanner, subscribers []*client.ModelSrvClient, log *zap.SugaredLogger) *Server {
	return &Server{
		scanners:     scanners,
		subscribers:  subscribers,
		pollInterval: time.Hour,
		log:          log,
		known:        make(map[uuid.UUID]struct{}),
	}
}

// ScanOnce exposes the internal scan method for testing.
func (s *Server) ScanOnce(ctx context.Context) {
	s.scan(ctx)
}

// KnownCount returns the number of tracked resource IDs (for test assertions).
func (s *Server) KnownCount() int {
	return len(s.known)
}
