// Package version exposes build information for RelayForge binaries.
//
// Version and Commit can be injected at build time:
//
//	go build -ldflags "-X relayforge/internal/version.Version=v1.2.3 -X relayforge/internal/version.Commit=abc123"
//
// When they are not injected, Get falls back to the VCS information the Go
// toolchain records when building from a git checkout.
package version

import "runtime/debug"

// Build-time variables; overridden via -ldflags -X.
var (
	Version = "dev"
	Commit  = "unknown"
)

// Info describes a single build of RelayForge.
type Info struct {
	Version   string // semantic version, or "dev"
	Commit    string // git revision, or "unknown"
	GoVersion string // toolchain version the binary was built with
}

// Get returns the build information for the running binary.
func Get() Info {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return Info{Version: Version, Commit: Commit}
	}
	return fromBuildInfo(Version, Commit, bi)
}

// fromBuildInfo merges the injected build variables with toolchain build
// info. Empty injected values are treated as unset. It is split out of Get
// so it can be tested with synthetic values.
func fromBuildInfo(version, commit string, bi *debug.BuildInfo) Info {
	if version == "" {
		version = "dev"
	}
	if commit == "" {
		commit = "unknown"
	}
	info := Info{Version: version, Commit: commit, GoVersion: bi.GoVersion}
	if info.Version == "dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		info.Version = bi.Main.Version
	}
	if info.Commit == "unknown" {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				info.Commit = s.Value
				break
			}
		}
	}
	return info
}

// String renders the build information as a compact human-readable string.
func (i Info) String() string {
	if i.Commit != "" && i.Commit != "unknown" {
		return i.Version + " (" + i.Commit + ")"
	}
	return i.Version
}
