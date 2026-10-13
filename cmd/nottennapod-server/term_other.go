//go:build !darwin && !linux

package main

import "errors"

// Elsewhere the passphrase comes from --passphrase-file.

const echoFlags = 0

type termios struct{ Lflag uint32 }

func getTermios(uintptr) (*termios, error) { return nil, errors.New("no terminal support") }

func setTermios(uintptr, *termios) error { return errors.New("no terminal support") }
