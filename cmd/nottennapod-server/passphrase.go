package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
)

// passphrase returns the key bundle's passphrase (PROTOCOL §11): from a
// file (without its final line break), or typed on the terminal without echo,
// twice when confirm is set.
func passphrase(file string, confirm bool, stdin *os.File, stderr io.Writer) ([]byte, error) {
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("cannot read the passphrase file: %w", err)
		}
		data = bytes.TrimSuffix(data, []byte("\n"))
		data = bytes.TrimSuffix(data, []byte("\r"))
		if len(data) == 0 {
			return nil, errors.New("the passphrase file is empty")
		}
		return data, nil
	}
	first, err := promptNoEcho(stdin, stderr, "Passphrase: ")
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, errors.New("the passphrase is empty")
	}
	if confirm {
		second, err := promptNoEcho(stdin, stderr, "Type the passphrase again: ")
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(first, second) {
			return nil, errors.New("the two passphrases differ")
		}
	}
	return first, nil
}

// promptNoEcho reads one line from the terminal with echo turned off, restoring
// the terminal afterwards, also when interrupted.
func promptNoEcho(in *os.File, stderr io.Writer, prompt string) ([]byte, error) {
	fd := in.Fd()
	old, err := getTermios(fd)
	if err != nil {
		return nil, errors.New("no terminal to read the passphrase from; use --passphrase-file")
	}
	quiet := *old
	quiet.Lflag &^= echoFlags
	if err := setTermios(fd, &quiet); err != nil {
		return nil, err
	}
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)
	done := make(chan struct{})
	go func() {
		select {
		case <-interrupted:
			_ = setTermios(fd, old)
			fmt.Fprintln(stderr)
			os.Exit(130)
		case <-done:
		}
	}()
	defer func() {
		close(done)
		signal.Stop(interrupted)
		_ = setTermios(fd, old)
		fmt.Fprintln(stderr)
	}()
	fmt.Fprint(stderr, prompt)
	var line []byte
	b := make([]byte, 1)
	for {
		n, err := in.Read(b)
		if n == 1 {
			if b[0] == '\n' || b[0] == '\r' {
				return line, nil
			}
			line = append(line, b[0])
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return line, nil
			}
			return nil, fmt.Errorf("reading the passphrase: %w", err)
		}
	}
}
