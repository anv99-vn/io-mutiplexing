package main

import "time"

const (
	idleTimeout = 30 * time.Second // idle conn closed after this
	sweepTick   = 1 * time.Second  // deadline sweep interval
)
