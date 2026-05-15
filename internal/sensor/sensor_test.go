package sensor_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.emeland.io/modelsrv/pkg/client"
	"go.uber.org/zap"

	"emeland.io/modelsrv-oci-registry-sensor/internal/scanner"
	"emeland.io/modelsrv-oci-registry-sensor/internal/sensor"
)

type mockScanner struct {
	images []scanner.Image
}

func (m *mockScanner) Scan(_ context.Context) ([]scanner.Image, error) {
	return m.images, nil
}

type pushedEvent struct {
	Kind       string `json:"kind"`
	Operation  string `json:"operation"`
	ResourceId string `json:"resourceId,omitempty"`
	Resource   map[string]any `json:"resource,omitempty"`
}

func TestScanOnce_EmitsArtefactAndInstance(t *testing.T) {
	var mu sync.Mutex
	var events []pushedEvent

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events/push" {
			body, _ := io.ReadAll(r.Body)
			var ev pushedEvent
			_ = json.Unmarshal(body, &ev)
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		// /test endpoint for client readiness
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := client.NewModelSrvClient(srv.URL + "/")
	require.NoError(t, err)

	mock := &mockScanner{
		images: []scanner.Image{
			{
				Repository: "registry.example.com/myapp",
				Digest:     "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				Tags:       []string{"v1.0", "latest"},
			},
		},
	}

	log := zap.NewNop().Sugar()
	s := sensor.NewTestServer([]sensor.ImageScanner{mock}, []*client.ModelSrvClient{c}, log)
	s.ScanOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, events, 2, "expected one Artefact + one ArtefactInstance event")

	// First event: Artefact
	assert.Equal(t, "Artifact", events[0].Kind)
	assert.Equal(t, "Create", events[0].Operation)
	res0 := events[0].Resource
	assert.Contains(t, res0["hash"], "SHA256:")
	assert.Equal(t, res0["displayName"], "v1.0 (sha256:abcdef123456...)") // first tag + truncated digest

	// Second event: ArtefactInstance
	assert.Equal(t, "ArtifactInstance", events[1].Kind)
	assert.Equal(t, "Create", events[1].Operation)
	res1 := events[1].Resource
	assert.Equal(t, "registry.example.com/myapp", res1["displayName"])
	assert.NotEmpty(t, res1["artifact"]) // references the artefact UUID
}

func TestScanOnce_DeterministicIDs(t *testing.T) {
	var mu sync.Mutex
	var events1, events2 []pushedEvent

	handler := func(events *[]pushedEvent) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/events/push" {
				body, _ := io.ReadAll(r.Body)
				var ev pushedEvent
				_ = json.Unmarshal(body, &ev)
				mu.Lock()
				*events = append(*events, ev)
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
		}
	}

	srv1 := httptest.NewServer(handler(&events1))
	defer srv1.Close()
	srv2 := httptest.NewServer(handler(&events2))
	defer srv2.Close()

	c1, _ := client.NewModelSrvClient(srv1.URL + "/")
	c2, _ := client.NewModelSrvClient(srv2.URL + "/")

	img := scanner.Image{
		Repository: "reg.io/app",
		Digest:     "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		Tags:       []string{"v2"},
	}

	log := zap.NewNop().Sugar()
	s1 := sensor.NewTestServer([]sensor.ImageScanner{&mockScanner{images: []scanner.Image{img}}}, []*client.ModelSrvClient{c1}, log)
	s2 := sensor.NewTestServer([]sensor.ImageScanner{&mockScanner{images: []scanner.Image{img}}}, []*client.ModelSrvClient{c2}, log)

	s1.ScanOnce(context.Background())
	s2.ScanOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	// Same image scanned twice should produce identical resource IDs.
	require.Len(t, events1, 2)
	require.Len(t, events2, 2)
	assert.Equal(t, events1[0].Resource["artifactId"], events2[0].Resource["artifactId"])
	assert.Equal(t, events1[1].Resource["artifactInstanceId"], events2[1].Resource["artifactInstanceId"])
}
