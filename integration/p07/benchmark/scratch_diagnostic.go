//go:build p12diagnostics

package main

import rados "github.com/otuschhoff/rados-go"

func configureScratch(client *rados.Client, slots int) error {
	return client.ConfigureP12ScratchDiagnostic(slots)
}

func scratchCounters(client *rados.Client) scratchStatistics {
	value := client.P12ScratchDiagnostic()
	return scratchStatistics{Hits: value.Hits, Misses: value.Misses, Bypasses: value.Bypasses, RetainedBytes: value.RetainedBytes}
}
