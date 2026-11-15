# Makefile untuk membangun binary statis waapi yang hemat memori
BINARY := waapi
PKG    := ./cmd/waapi
LDFLAGS := -s -w -buildid=

.PHONY: build build-linux build-linux-arm64 vet test test-race memprobe clean

# Build untuk sistem operasi saat ini
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Build khusus produksi Linux amd64
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Build khusus produksi Linux arm64
build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Jalankan analisis statis Go
vet:
	CGO_ENABLED=0 go vet ./...

# Jalankan seluruh unit dan integration test tanpa CGO
test:
	CGO_ENABLED=0 go test -v ./...

# Jalankan seluruh test dengan race detector (butuh CGO)
test-race:
	go test -v -race ./...

# Pengukuran memori idle ke JSON (memerlukan lingkungan Linux /proc)
memprobe:
	bash scripts/memprobe.sh HEAD bench/memprobe-baseline.json

# Bersihkan hasil kompilasi
clean:
	rm -rf bin/

