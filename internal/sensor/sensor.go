// Package sensor bridges the OCI scanner output to modelsrv event emission.
package sensor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.emeland.io/modelsrv/pkg/client"
	"go.emeland.io/modelsrv/pkg/events"
	"go.uber.org/zap"

	"emeland.io/modelsrv-oci-registry-sensor/internal/config"
	"emeland.io/modelsrv-oci-registry-sensor/internal/scanner"
)

// artefactNamespace is the UUID v5 namespace for deriving deterministic Artefact IDs from digests.
var artefactNamespace = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// Server manages periodic scanning and event forwarding.
type Server struct {
	scanners     []*scanner.Scanner
	subscribers  []*client.ModelSrvClient
	pollInterval time.Duration
	log          *zap.SugaredLogger
}

// New creates a sensor server from the given config.
func New(cfg *config.Config, log *zap.SugaredLogger) (*Server, error) {
	poll := 60 * time.Second
	if cfg.PollInterval != "" {
		d, err := time.ParseDuration(cfg.PollInterval)
		if err != nil {
			return nil, fmt.Errorf("parse pollInterval: %w", err)
		}
		poll = d
	}

	var scanners []*scanner.Scanner
	for _, reg := range cfg.Registries {
		scanners = append(scanners, scanner.New(reg.URL, reg.Username, reg.Password, log))
	}

	var subscribers []*client.ModelSrvClient
	for _, url := range cfg.Subscribers {
		c, err := client.NewModelSrvClient(url)
		if err != nil {
			return nil, fmt.Errorf("create client for %s: %w", url, err)
		}
		subscribers = append(subscribers, c)
	}

	return &Server{
		scanners:     scanners,
		subscribers:  subscribers,
		pollInterval: poll,
		log:          log,
	}, nil
}

// Run starts the periodic scan loop until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.log.Infow("starting OCI registry sensor", "pollInterval", s.pollInterval, "registries", len(s.scanners))

	// Initial scan.
	s.scan(ctx)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("shutting down")
			return nil
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

func (s *Server) scan(ctx context.Context) {
	for _, sc := range s.scanners {
		images, err := sc.Scan(ctx)
		if err != nil {
			s.log.Warnw("scan failed", "error", err)
			continue
		}
		s.log.Infow("scan complete", "images", len(images))
		for _, img := range images {
			s.emitArtefact(ctx, img)
		}
	}
}

func (s *Server) emitArtefact(ctx context.Context, img scanner.Image) {
	artefactID := artefactIDFromDigest(img.Digest)
	instanceID := uuid.NewSHA1(artefactNamespace, []byte(img.Repository+"|"+img.Digest))

	// Build tags annotation.
	tagsJSON, _ := json.Marshal(img.Tags)

	// Emit Artefact (create/update).
	artefactEvent := &events.Event{
		ResourceType: events.ArtifactResource,
		Operation:    events.CreateOperation,
		ResourceId:   artefactID,
		Objects:      []any{artefactPayload(artefactID, img.Digest, img.Tags)},
	}

	// Emit ArtefactInstance.
	instanceEvent := &events.Event{
		ResourceType: events.ArtifactInstanceResource,
		Operation:    events.CreateOperation,
		ResourceId:   instanceID,
		Objects:      []any{instancePayload(instanceID, artefactID, img.Repository, string(tagsJSON))},
	}

	for _, c := range s.subscribers {
		if err := c.PostEvent(ctx, artefactEvent); err != nil {
			s.log.Warnw("failed to push artefact event", "id", artefactID, "error", err)
		}
		if err := c.PostEvent(ctx, instanceEvent); err != nil {
			s.log.Warnw("failed to push instance event", "id", instanceID, "error", err)
		}
	}
}

// artefactIDFromDigest derives a deterministic UUID from the image digest.
func artefactIDFromDigest(digest string) uuid.UUID {
	return uuid.NewSHA1(artefactNamespace, []byte(digest))
}

func artefactPayload(id uuid.UUID, digest string, tags []string) map[string]any {
	// Normalize digest to "SHA256:<hex>" format.
	hash := digest
	if strings.HasPrefix(digest, "sha256:") {
		hash = "SHA256:" + strings.TrimPrefix(digest, "sha256:")
	}
	return map[string]any{
		"artifactId":  id.String(),
		"displayName": tags[0] + " (" + digest[:19] + "...)",
		"hash":        hash,
		"annotations": []map[string]any{
			{"key": "emeland.io/oci-registry-sensor/known-tags", "value": mustJSON(tags)},
		},
	}
}

func instancePayload(id, artefactID uuid.UUID, repository, tagsJSON string) map[string]any {
	return map[string]any{
		"artifactInstanceId": id.String(),
		"displayName":        repository,
		"artifact":           artefactID.String(),
		"annotations": []map[string]any{
			{"key": "emeland.io/p8-artifact-instance-location", "value": mustJSON([]string{repository})},
			{"key": "emeland.io/oci-registry-sensor/known-tags", "value": tagsJSON},
		},
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
