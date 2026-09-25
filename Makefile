# RelayForge Makefile (Phase 1).
#
# The repository intentionally contains ZERO .go files at the end of Phase 1
# (labs live in ~/rf-labs). On a module with no packages, `go vet` and
# `go test` exit non-zero ("no packages to ..."), so the package targets are
# guarded: they print an honest no-op note now and run the real command the
# moment Go code exists (Phase 3 adds internal/logengine).

PATH := /usr/local/go/bin:$(PATH)
export PATH

HAS_GO_FILES := $(shell find . -name '*.go' -not -path './.git/*' | head -1)

.PHONY: build test race vet fmt

build:
ifdef HAS_GO_FILES
	go build ./...
else
	@echo "build: no Go packages yet (zero .go files by design in Phase 1)"
endif

test:
ifdef HAS_GO_FILES
	go test ./...
else
	@echo "test: no Go packages yet (zero .go files by design in Phase 1)"
endif

race:
ifdef HAS_GO_FILES
	go test -race ./...
else
	@echo "race: no Go packages yet (zero .go files by design in Phase 1)"
endif

vet:
ifdef HAS_GO_FILES
	go vet ./...
else
	@echo "vet: no Go packages yet (zero .go files by design in Phase 1)"
endif
	gofmt -l .

fmt:
	gofmt -w .
