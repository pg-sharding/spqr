package testutil

import "time"

// nolint: unparam
func Retry(code func() bool, timeout, sleep time.Duration) bool {
	if code() {
		return true
	}
	timer := time.NewTimer(timeout)
	ticker := time.NewTicker(sleep)
	for {
		select {
		case <-ticker.C:
			if code() {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}
