// Package scanner provides OCI registry scanning to discover container images.
package scanner

import (
	"context"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"go.uber.org/zap"
)

// Image represents a discovered container image in a registry.
type Image struct {
	// Repository is the full repository name (e.g. "registry.example.com/myapp").
	Repository string
	// Digest is the SHA256 manifest digest (e.g. "sha256:abc123...").
	Digest string
	// Tags are the tags pointing to this digest.
	Tags []string
}

// Scanner scans an OCI registry for images.
type Scanner struct {
	registryURL string
	auth        authn.Authenticator
	log         *zap.SugaredLogger
}

// New creates a scanner for the given registry URL with optional credentials.
func New(registryURL, username, password string, log *zap.SugaredLogger) *Scanner {
	auth := authn.Anonymous
	if username != "" {
		auth = authn.FromConfig(authn.AuthConfig{
			Username: username,
			Password: password,
		})
	}
	return &Scanner{registryURL: registryURL, auth: auth, log: log}
}

// Scan lists all repositories in the registry and discovers images with their digests and tags.
func (s *Scanner) Scan(ctx context.Context) ([]Image, error) {
	reg, err := name.NewRegistry(s.registryURL)
	if err != nil {
		return nil, fmt.Errorf("parse registry %s: %w", s.registryURL, err)
	}

	repos, err := remote.Catalog(ctx, reg, remote.WithAuth(s.auth))
	if err != nil {
		return nil, fmt.Errorf("catalog %s: %w", s.registryURL, err)
	}

	var images []Image
	for _, repoName := range repos {
		repoImages, err := s.scanRepository(ctx, repoName)
		if err != nil {
			s.log.Warnw("failed to scan repository", "repo", repoName, "error", err)
			continue
		}
		images = append(images, repoImages...)
	}
	return images, nil
}

func (s *Scanner) scanRepository(ctx context.Context, repoName string) ([]Image, error) {
	repo, err := name.NewRepository(fmt.Sprintf("%s/%s", s.registryURL, repoName))
	if err != nil {
		return nil, err
	}

	tags, err := remote.List(repo, remote.WithAuth(s.auth), remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list tags %s: %w", repoName, err)
	}

	// Group tags by digest.
	digestTags := make(map[string][]string)
	for _, tag := range tags {
		ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", s.registryURL, repoName, tag))
		if err != nil {
			continue
		}
		desc, err := remote.Head(ref, remote.WithAuth(s.auth), remote.WithContext(ctx))
		if err != nil {
			s.log.Debugw("failed to resolve tag", "repo", repoName, "tag", tag, "error", err)
			continue
		}
		digest := desc.Digest.String()
		digestTags[digest] = append(digestTags[digest], tag)
	}

	var images []Image
	for digest, tagList := range digestTags {
		images = append(images, Image{
			Repository: fmt.Sprintf("%s/%s", s.registryURL, repoName),
			Digest:     digest,
			Tags:       tagList,
		})
	}
	return images, nil
}
