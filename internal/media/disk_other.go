//go:build !darwin && !linux

package media

// freeSpace is unknown on other systems: the copy's own cap still applies.
func freeSpace(string) (int64, bool) { return 0, false }
