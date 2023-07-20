package testutil

import "time"

// nolint: unparam
func Retry(code func() bool, timeout, sleep time.Duration) {
	if code() {
		return
	}
	timer := time.NewTimer(timeout)
	ticker := time.NewTicker(sleep)
	for {
		select {
		case <-ticker.C:
			if code() {
				return
			}
		case <-timer.C:
			return
		}
	}
}
