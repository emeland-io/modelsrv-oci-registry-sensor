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
	Kind       string         `json:"kind"`
	Operation  string         `json:"operation"`
	ResourceId string         `json:"resourceId,omitempty"`
	Resource   map[string]any `json:"resource,omitempty"`
}

func collectEvents(t *testing.T) (*httptest.Server, *[]*pushedEvent, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var evts []*pushedEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events/push" {
			body, _ := io.ReadAll(r.Body)
			var ev pushedEvent
			_ = json.Unmarshal(body, &ev)
			mu.Lock()
			evts = append(evts, &ev)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	return srv, &evts, &mu
}

func TestScanOnce_EmitsArtefactAndInstance(t *testing.T) {
	srv, evts, mu := collectEvents(t)
	defer srv.Close()

	c, err := client.NewModelSrvClient(srv.URL + "/")
	require.NoError(t, err)

	mock := &mockScanner{images: []scanner.Image{
		{
			Repository: "registry.example.com/myapp",
			Digest:     "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			Tags:       []string{"v1.0", "latest"},
		},
	}}

	log := zap.NewNop().Sugar()
	s := sensor.NewTestServer([]sensor.ImageScanner{mock}, []*client.ModelSrvClient{c}, log)
	s.ScanOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, *evts, 2)
	assert.Equal(t, "Artifact", (*evts)[0].Kind)
	assert.Equal(t, "Create", (*evts)[0].Operation)
	assert.Contains(t, (*evts)[0].Resource["hash"], "SHA256:")
	assert.Contains(t, (*evts)[0].Resource["displayName"], "v1.0")
	assert.NotEmpty(t, (*evts)[0].Resource["description"])

	assert.Equal(t, "ArtifactInstance", (*evts)[1].Kind)
	assert.Equal(t, "Create", (*evts)[1].Operation)
	assert.Equal(t, "registry.example.com/myapp", (*evts)[1].Resource["displayName"])
	assert.NotEmpty(t, (*evts)[1].Resource["description"])
	assert.NotEmpty(t, (*evts)[1].Resource["artifact"])
}

func TestScanOnce_UntaggedImage(t *testing.T) {
	srv, evts, mu := collectEvents(t)
	defer srv.Close()

	c, _ := client.NewModelSrvClient(srv.URL + "/")
	mock := &mockScanner{images: []scanner.Image{
		{
			Repository: "reg.io/lib",
			Digest:     "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			Tags:       nil, // untagged
		},
	}}

	log := zap.NewNop().Sugar()
	s := sensor.NewTestServer([]sensor.ImageScanner{mock}, []*client.ModelSrvClient{c}, log)
	s.ScanOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, *evts, 2)
	// displayName should not panic, should use digest prefix
	assert.Contains(t, (*evts)[0].Resource["displayName"], "sha256:00000000000")
}

func TestScanOnce_SecondScanEmitsUpdate(t *testing.T) {
	srv, evts, mu := collectEvents(t)
	defer srv.Close()

	c, _ := client.NewModelSrvClient(srv.URL + "/")
	img := scanner.Image{
		Repository: "reg.io/app",
		Digest:     "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		Tags:       []string{"v2"},
	}
	mock := &mockScanner{images: []scanner.Image{img}}

	log := zap.NewNop().Sugar()
	s := sensor.NewTestServer([]sensor.ImageScanner{mock}, []*client.ModelSrvClient{c}, log)

	s.ScanOnce(context.Background()) // first scan: Create
	s.ScanOnce(context.Background()) // second scan: Update

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, *evts, 4) // 2 creates + 2 updates
	assert.Equal(t, "Create", (*evts)[0].Operation)
	assert.Equal(t, "Create", (*evts)[1].Operation)
	assert.Equal(t, "Update", (*evts)[2].Operation)
	assert.Equal(t, "Update", (*evts)[3].Operation)
}

func TestScanOnce_DeleteOnRemoval(t *testing.T) {
	srv, evts, mu := collectEvents(t)
	defer srv.Close()

	c, _ := client.NewModelSrvClient(srv.URL + "/")
	img := scanner.Image{
		Repository: "reg.io/app",
		Digest:     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		Tags:       []string{"v1"},
	}
	mock := &mockScanner{images: []scanner.Image{img}}

	log := zap.NewNop().Sugar()
	s := sensor.NewTestServer([]sensor.ImageScanner{mock}, []*client.ModelSrvClient{c}, log)

	s.ScanOnce(context.Background()) // first scan: image present
	assert.Equal(t, 2, s.KnownCount())

	// Image disappears from registry.
	mock.images = nil
	s.ScanOnce(context.Background()) // second scan: image gone

	mu.Lock()
	defer mu.Unlock()

	// Should have: 2 creates + delete events for both IDs (2 resource types × 2 IDs = 4 deletes)
	var deletes int
	for _, ev := range *evts {
		if ev.Operation == "Delete" {
			deletes++
		}
	}
	assert.True(t, deletes > 0, "expected delete events after image removal")
	assert.Equal(t, 0, s.KnownCount())
}

func TestDeterministicIDs(t *testing.T) {
	srv, evts, mu := collectEvents(t)
	defer srv.Close()

	c, _ := client.NewModelSrvClient(srv.URL + "/")
	img := scanner.Image{
		Repository: "reg.io/app",
		Digest:     "sha256:3333333333333333333333333333333333333333333333333333333333333333",
		Tags:       []string{"latest"},
	}

	log := zap.NewNop().Sugar()
	s1 := sensor.NewTestServer([]sensor.ImageScanner{&mockScanner{images: []scanner.Image{img}}}, []*client.ModelSrvClient{c}, log)
	s1.ScanOnce(context.Background())

	s2 := sensor.NewTestServer([]sensor.ImageScanner{&mockScanner{images: []scanner.Image{img}}}, []*client.ModelSrvClient{c}, log)
	s2.ScanOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, *evts, 4)
	assert.Equal(t, (*evts)[0].Resource["artifactId"], (*evts)[2].Resource["artifactId"])
	assert.Equal(t, (*evts)[1].Resource["artifactInstanceId"], (*evts)[3].Resource["artifactInstanceId"])
}
