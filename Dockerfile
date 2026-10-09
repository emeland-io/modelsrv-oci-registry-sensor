# Build the binary
FROM golang:1.27 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o modelsrv-oci-registry-sensor ./cmd/modelsrv-oci-registry-sensor

# Use distroless as minimal base image
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/modelsrv-oci-registry-sensor .
USER 65532:65532

ENTRYPOINT ["/modelsrv-oci-registry-sensor"]
