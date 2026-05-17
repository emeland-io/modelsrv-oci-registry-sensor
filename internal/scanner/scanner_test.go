package scanner_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/google/go-containerregistry/pkg/v1/types"

	"emeland.io/modelsrv-oci-registry-sensor/internal/scanner"
)

func TestScan_FakeRegistry(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	defer srv.Close()
	regHost := strings.TrimPrefix(srv.URL, "http://")

	pushImage(t, regHost, "myapp", "v1.0")
	pushImage(t, regHost, "myapp", "v1.1")
	pushImage(t, regHost, "library/nginx", "latest")

	log := zap.NewNop().Sugar()
	sc := scanner.New(regHost, "", "", log)
	images, err := sc.Scan(context.Background())
	require.NoError(t, err)

	assert.GreaterOrEqual(t, len(images), 2)
	for _, img := range images {
		assert.NotEmpty(t, img.Digest)
		assert.NotEmpty(t, img.Tags)
		assert.True(t, strings.HasPrefix(img.Digest, "sha256:"))
		assert.Contains(t, img.Repository, regHost)
	}
}

func TestScan_EmptyRegistry(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	defer srv.Close()
	regHost := strings.TrimPrefix(srv.URL, "http://")

	log := zap.NewNop().Sugar()
	sc := scanner.New(regHost, "", "", log)
	images, err := sc.Scan(context.Background())
	require.NoError(t, err)
	assert.Empty(t, images)
}

func TestScan_MultipleTagsSameDigest(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	defer srv.Close()
	regHost := strings.TrimPrefix(srv.URL, "http://")

	img := buildImage(t)
	pushImageRef(t, regHost, "shared", "v1", img)
	pushImageRef(t, regHost, "shared", "latest", img)

	log := zap.NewNop().Sugar()
	sc := scanner.New(regHost, "", "", log)
	images, err := sc.Scan(context.Background())
	require.NoError(t, err)

	var sharedImages []scanner.Image
	for _, i := range images {
		if strings.Contains(i.Repository, "shared") {
			sharedImages = append(sharedImages, i)
		}
	}
	require.Len(t, sharedImages, 1, "same digest should produce one image entry")
	assert.Len(t, sharedImages[0].Tags, 2)
	assert.Contains(t, sharedImages[0].Tags, "v1")
	assert.Contains(t, sharedImages[0].Tags, "latest")
}

func pushImage(t *testing.T, regHost, repo, tag string) {
	t.Helper()
	img := buildImage(t)
	pushImageRef(t, regHost, repo, tag, img)
}

func pushImageRef(t *testing.T, regHost, repo, tag string, img v1.Image) {
	t.Helper()
	ref, err := name.ParseReference(regHost+"/"+repo+":"+tag, name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, img))
}

func buildImage(t *testing.T) v1.Image {
	t.Helper()
	layer, err := random.Layer(256, types.OCILayer)
	require.NoError(t, err)
	img, err := mutate.AppendLayers(empty.Image, layer)
	require.NoError(t, err)
	return img
}
