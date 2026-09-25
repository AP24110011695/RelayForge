package version

import (
	"runtime/debug"
	"testing"
)

func TestInfoString(t *testing.T) {
	tests := []struct {
		name string
		info Info
		want string
	}{
		{"dev build", Info{Version: "dev"}, "dev"},
		{"dev build with unknown commit", Info{Version: "dev", Commit: "unknown"}, "dev"},
		{"release build with commit", Info{Version: "v1.2.3", Commit: "abc1234"}, "v1.2.3 (abc1234)"},
		{"release build without commit", Info{Version: "v1.2.3"}, "v1.2.3"},
		{"empty info", Info{}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.info.String(); got != tc.want {
				t.Errorf("Info.String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFromBuildInfo(t *testing.T) {
	tests := []struct {
		name    string
		version string
		commit  string
		bi      *debug.BuildInfo
		want    Info
	}{
		{
			name:    "injected values take precedence",
			version: "v1.0.0",
			commit:  "aaaa",
			bi: &debug.BuildInfo{
				Main:     debug.Module{Version: "v9.9.9"},
				Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "bbbb"}},
			},
			want: Info{Version: "v1.0.0", Commit: "aaaa"},
		},
		{
			name: "dev version falls back to main module version",
			bi:   &debug.BuildInfo{Main: debug.Module{Version: "v0.3.0"}},
			want: Info{Version: "v0.3.0", Commit: "unknown"},
		},
		{
			name: "devel main version is ignored",
			bi:   &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}},
			want: Info{Version: "dev", Commit: "unknown"},
		},
		{
			name: "unknown commit falls back to vcs revision",
			bi:   &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "deadbeef"}}},
			want: Info{Version: "dev", Commit: "deadbeef"},
		},
		{
			name: "bare build info",
			bi:   &debug.BuildInfo{},
			want: Info{Version: "dev", Commit: "unknown"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fromBuildInfo(tc.version, tc.commit, tc.bi)
			if got != tc.want {
				t.Errorf("fromBuildInfo() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestGet(t *testing.T) {
	got := Get()
	if got.Version == "" {
		t.Fatal("Get().Version is empty")
	}
	if got.GoVersion == "" {
		t.Error("Get().GoVersion is empty: expected build info under go test")
	}
	if got.String() == "" {
		t.Error("Get().String() is empty")
	}
}
