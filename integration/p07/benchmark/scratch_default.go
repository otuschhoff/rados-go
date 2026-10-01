//go:build !p12diagnostics

package main

import (
	"fmt"
	rados "github.com/otuschhoff/rados-go"
)

func configureScratch(*rados.Client, int) error {
	return fmt.Errorf("scratch experiments require p12diagnostics build tag")
}

func scratchCounters(*rados.Client) scratchStatistics { return scratchStatistics{} }
