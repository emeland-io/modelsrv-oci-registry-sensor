package sensor

import (
	"context"
	"time"

	"go.emeland.io/modelsrv/pkg/client"
	"go.uber.org/zap"
)

// NewTestServer creates a Server with injected scanners and subscribers for testing.
func NewTestServer(scanners []ImageScanner, subscribers []*client.ModelSrvClient, log *zap.SugaredLogger) *Server {
	return &Server{
		scanners:     scanners,
		subscribers:  subscribers,
		pollInterval: time.Hour, // won't tick in tests
		log:          log,
	}
}

// ScanOnce exposes the internal scan method for testing.
func (s *Server) ScanOnce(ctx context.Context) {
	s.scan(ctx)
}
