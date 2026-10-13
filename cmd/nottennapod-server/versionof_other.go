//go:build !unix

package main

import "os/exec"

func ownGroup(*exec.Cmd) {}
func endGroup(*exec.Cmd) {}
