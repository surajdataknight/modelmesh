# syntax=docker/dockerfile:1
# Multi-stage build: static binaries on an empty base image, running as a
# non-root user. Works with Docker Engine or Podman (both free).
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ ./cmd/... \
 && apk add --no-cache ca-certificates

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ /usr/local/bin/
COPY configs/ /etc/modelmesh/
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/modelmesh"]
CMD ["-config", "/etc/modelmesh/compose.json"]
