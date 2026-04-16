package topology

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const defaultShardHostValidationTimeout = 3 * time.Second

func ValidateDataShardHosts(ctx context.Context, shard *DataShard) error {
	if shard == nil {
		return spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "shard definition is nil")
	}

	hosts := retrieveRawHostsFromOptions(shard.options)
	if len(hosts) == 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard %q has no valid hosts configured", shard.ID)
	}
	if len(hosts) != len(shard.Hosts()) {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard %q has invalid or unsupported host definitions", shard.ID)
	}

	dialer := &net.Dialer{Timeout: defaultShardHostValidationTimeout}
	errCh := make(chan error, len(hosts))
	var wg sync.WaitGroup

	for _, host := range hosts {
		wg.Add(1)
		go func(h string) {
			defer wg.Done()
			conn, err := dialer.DialContext(ctx, "tcp", h)
			if err != nil {
				errCh <- spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard host %q is not reachable: %v", h, err)
				return
			}
			if err := conn.Close(); err != nil {
				spqrlog.Zero.Warn().Err(err).Str("host", h).Msg("failed to close validation connection to shard host")
			}
		}(host)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}
