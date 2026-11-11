# Tahap build: kompilasi binary statis tanpa CGO
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -buildid=" -o /out/waapi ./cmd/waapi

# Tahap runtime: distroless static image tanpa shell
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/waapi /waapi
ENV WAAPI_DATA_DIR=/data GOMEMLIMIT=80MiB GOGC=50 GOMAXPROCS=2
VOLUME ["/data"]
ENTRYPOINT ["/waapi", "serve"]

