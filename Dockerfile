# RelayForge broker image: multi-stage build, static binary on distroless.
# NOTE (Phase 8): authored and reviewed but NOT verified live — Docker
# Desktop WSL integration was disabled in the reference environment at
# execution time (docs/ENVIRONMENT.md). Run `docker build .` after enabling
# it; report any gap rather than trusting this file blindly.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X relayforge/internal/version.Version=$(git describe --tags --always 2>/dev/null || echo dev)" \
    -o /out/rfbroker ./cmd/rfbroker && \
    CGO_ENABLED=0 go build -trimpath -o /out/relayctl ./cmd/relayctl && \
    CGO_ENABLED=0 go build -trimpath -o /out/rfbench ./cmd/rfbench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rfbroker /rfbroker
COPY --from=build /out/relayctl /relayctl
COPY --from=build /out/rfbench /rfbench
EXPOSE 7000 7001
VOLUME ["/data"]
ENTRYPOINT ["/rfbroker"]
CMD ["-addr", ":7000", "-admin-addr", ":7001", "-data-dir", "/data"]
