.PHONY: build test clean

build:
	go build -o bin/modelsrv-oci-registry-sensor ./cmd/modelsrv-oci-registry-sensor

test:
	go test ./...

clean:
	rm -rf bin/
