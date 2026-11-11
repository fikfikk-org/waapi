# Makefile untuk membangun binary statis waapi yang hemat memori
BINARY := waapi
PKG    := ./cmd/waapi
LDFLAGS := -s -w -buildid=

.PHONY: build build-linux test clean

# Build untuk sistem operasi saat ini
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Build khusus produksi Linux amd64
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Jalankan seluruh unit dan integration test
test:
	go test -v -race ./...

# Bersihkan hasil kompilasi
clean:
	rm -rf bin/

