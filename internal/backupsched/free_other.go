//go:build !darwin && !linux

package backupsched

import "errors"

func freeBytes(string) (int64, error) { return 0, errors.New("free space unknown") }
