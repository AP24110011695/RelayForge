package bench

import "runtime"

func runtimeArch() string { return runtime.GOARCH }
func runtimeCPU() int     { return runtime.NumCPU() }
func osName() string      { return runtime.GOOS }
