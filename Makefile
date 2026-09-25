# RelayForge Makefile.
#
# Run from the WSL2 ext4 checkout (see docs/ENVIRONMENT.md). GO is pinned to
# /usr/local/go/bin because that is where the toolchain lives; it falls back
# to PATH if the pin disappears.

GO := /usr/local/go/bin/go
ifeq ($(wildcard /usr/local/go/bin/go),)
GO := go
endif

FUZZTIME ?= 15s

.PHONY: build test test-race vet fmt fuzz

build:
	$(GO) build ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...
	gofmt -l .

fmt:
	gofmt -l -w .

# Runs each Fuzz* target for FUZZTIME, one invocation per target (go test
# -fuzz only accepts a single target per run). Prints a note until the first
# fuzz target lands in Phase 2.
fuzz:
	@status=0; found=0; \
	for pkg in $$($(GO) list ./...); do \
		if [ "$$pkg" = "relayforge" ]; then dir=.; else dir=./$${pkg#relayforge/}; fi; \
		for t in $$($(GO) test -list '^Fuzz' $$dir 2>/dev/null | grep '^Fuzz' || true); do \
			found=1; \
			echo ">> fuzzing $$t ($$dir) for $(FUZZTIME)"; \
			$(GO) test $$dir -run='^$$' -fuzz="^$${t}$$" -fuzztime=$(FUZZTIME) || status=1; \
		done; \
	done; \
	[ $$found -eq 1 ] || echo "no fuzz targets yet (the first arrives in Phase 2)"; \
	exit $$status
