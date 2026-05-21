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

// artefactNamespace is the UUID v5 namespace for deriving deterministic IDs from digests.
// This value is fixed and must never change — doing so would reassign all resource UUIDs.
var artefactNamespace = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// ImageScanner abstracts registry scanning for testability.
type ImageScanner interface {
	Scan(ctx context.Context) ([]scanner.Image, error)
}

// Server manages periodic scanning and event forwarding.
type Server struct {
	scanners     []registryScanner
	subscribers  []*client.ModelSrvClient
	pollInterval time.Duration
	log          *zap.SugaredLogger
	// knownByRegistry tracks previously emitted resource IDs per registry for reconciliation.
	knownByRegistry map[string]map[uuid.UUID]struct{}
}

type registryScanner struct {
	url     string
	scanner ImageScanner
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

	var scanners []registryScanner
	for _, reg := range cfg.Registries {
		scanners = append(scanners, registryScanner{
			url:     reg.URL,
			scanner: scanner.New(reg.URL, reg.Username, reg.Password, log),
		})
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
		scanners:        scanners,
		subscribers:     subscribers,
		pollInterval:    poll,
		log:             log,
		knownByRegistry: make(map[string]map[uuid.UUID]struct{}),
	}, nil
}

// Run starts the periodic scan loop until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.log.Infow("starting OCI registry sensor", "pollInterval", s.pollInterval, "registries", len(s.scanners))

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
	for i := range s.scanners {
		rs := &s.scanners[i]
		images, err := rs.scanner.Scan(ctx)
		if err != nil {
			s.log.Warnw("scan failed", "registry", rs.url, "error", err)
			// Keep previous known IDs for this registry — don't reconcile on failure.
			continue
		}
		s.log.Infow("scan complete", "registry", rs.url, "images", len(images))

		seen := make(map[uuid.UUID]struct{})
		for _, img := range images {
			if len(img.Tags) == 0 && img.Digest == "" {
				continue
			}
			artID, instID := s.emitArtefact(ctx, rs.url, img)
			seen[artID] = struct{}{}
			seen[instID] = struct{}{}
		}

		// Reconcile: delete IDs previously known for this registry but not seen now.
		for id := range s.knownByRegistry[rs.url] {
			if _, ok := seen[id]; !ok {
				s.emitDelete(ctx, id)
			}
		}
		s.knownByRegistry[rs.url] = seen
	}
}

func (s *Server) emitArtefact(ctx context.Context, regURL string, img scanner.Image) (uuid.UUID, uuid.UUID) {
	artefactID := artefactIDFromDigest(img.Digest)
	instanceID := uuid.NewSHA1(artefactNamespace, []byte(img.Repository+"|"+img.Digest))

	tagsJSON, _ := json.Marshal(img.Tags)

	known := s.knownByRegistry[regURL]

	op := events.CreateOperation
	if _, exists := known[artefactID]; exists {
		op = events.UpdateOperation
	}

	artefactEvent := &events.Event{
		ResourceType: events.ArtifactResource,
		Operation:    op,
		ResourceId:   artefactID,
		Objects:      []any{artefactPayload(artefactID, img)},
	}

	instanceOp := events.CreateOperation
	if _, exists := known[instanceID]; exists {
		instanceOp = events.UpdateOperation
	}

	instanceEvent := &events.Event{
		ResourceType: events.ArtifactInstanceResource,
		Operation:    instanceOp,
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
	return artefactID, instanceID
}

func (s *Server) emitDelete(ctx context.Context, id uuid.UUID) {
	// We don't know if it's an Artefact or ArtefactInstance from the ID alone,
	// but modelsrv's Apply ignores deletes for unknown IDs gracefully.
	// Emit both; only the matching one will have effect.
	for _, rt := range []events.ResourceType{events.ArtifactResource, events.ArtifactInstanceResource} {
		ev := &events.Event{
			ResourceType: rt,
			Operation:    events.DeleteOperation,
			ResourceId:   id,
		}
		for _, c := range s.subscribers {
			if err := c.PostEvent(ctx, ev); err != nil {
				s.log.Debugw("delete push failed (may be expected)", "id", id, "kind", rt, "error", err)
			}
		}
	}
}

func artefactIDFromDigest(digest string) uuid.UUID {
	return uuid.NewSHA1(artefactNamespace, []byte(digest))
}

func artefactPayload(id uuid.UUID, img scanner.Image) map[string]any {
	hash := img.Digest
	if strings.HasPrefix(img.Digest, "sha256:") {
		hash = "SHA256:" + strings.TrimPrefix(img.Digest, "sha256:")
	}
	displayName := img.Digest[:19] + "..."
	if len(img.Tags) > 0 {
		displayName = img.Tags[0] + " (" + displayName + ")"
	}
	description := fmt.Sprintf("OCI image from %s", img.Repository)
	return map[string]any{
		"artifactId":  id.String(),
		"displayName": displayName,
		"description": description,
		"hash":        hash,
		"annotations": []map[string]any{
			{"key": "emeland.io/oci-registry-sensor/known-tags", "value": mustJSON(img.Tags)},
		},
	}
}

func instancePayload(id, artefactID uuid.UUID, repository, tagsJSON string) map[string]any {
	return map[string]any{
		"artifactInstanceId": id.String(),
		"displayName":        repository,
		"description":        fmt.Sprintf("Copy in registry %s", repository),
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
