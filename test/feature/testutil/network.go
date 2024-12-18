package testutil

import (
	"fmt"
	"net"
)

// GetFreePort returns a random tcp port available for binding (at the moment of call)
func GetFreePort() (string, error) {
	const tries = 3
	var errs []error

	for range tries {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to bind on free port at localhost: %w", err))
			continue
		}

		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to parse host/port string (%s): %w", listener.Addr().String(), err))
			continue
		}

		if err = listener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close listener: %w", err))
			continue
		}
		return port, nil
	}

	// TODO: use multierr when available
	return "", fmt.Errorf("unable to find free port with %d tries: %+v", tries, errs)
}
