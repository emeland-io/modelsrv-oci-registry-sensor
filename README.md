# modelsrv-oci-registry-sensor

Scans OCI-compliant container registries and reports discovered images as `Artefact` and `ArtefactInstance` resources to a [modelsrv](https://github.com/emeland-io/modelsrv) instance.

## Usage

```bash
make build
./bin/modelsrv-oci-registry-sensor -config config/sensor.yaml
```

## Configuration

See `config/sensor.yaml` for an example. Key fields:

- `subscribers` — modelsrv API base URLs to push events to
- `pollInterval` — how often to re-scan (e.g. `"60s"`, `"5m"`)
- `registries` — list of OCI registries with URL and optional credentials
  - `repositories` — optional list of repo paths (e.g. `emeland-io/modelsrv`). When set, skips full-registry catalog (required for GHCR). When empty, catalogs the entire registry.

## How it works

1. Catalogs all repositories in each configured registry
2. Lists tags per repository and resolves each to a manifest digest
3. Groups tags by digest — each unique digest becomes an `Artefact` (hash = SHA256 of manifest)
4. Each registry location becomes an `ArtefactInstance`
5. Pushes create events to all configured subscribers via POST /events/push

## License

Apache License 2.0
