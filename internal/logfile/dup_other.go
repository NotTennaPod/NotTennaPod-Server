//go:build !darwin && !linux

package logfile

// The server runs as a service only on macOS and Linux; elsewhere the log is
// written but standard output and error stay where they are.
func dupStd(int) error { return nil }
