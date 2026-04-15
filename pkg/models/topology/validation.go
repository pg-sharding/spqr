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
		return spqrerror.New(spqrerror.SpqrInvalidRequest, "shard definition is nil")
	}
	if shard.Cfg == nil {
		return spqrerror.Newf(spqrerror.SpqrInvalidRequest, "shard %q has no config", shard.ID)
	}

	hosts := shard.Cfg.Hosts()
	if len(shard.Cfg.RawHosts) != 0 && len(hosts) != len(shard.Cfg.RawHosts) {
		return spqrerror.Newf(spqrerror.SpqrInvalidRequest, "shard %q has invalid or unsupported host definitions", shard.ID)
	}
	if len(hosts) == 0 {
		return spqrerror.Newf(spqrerror.SpqrInvalidRequest, "shard %q has no valid hosts configured", shard.ID)
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
				errCh <- spqrerror.Newf(spqrerror.SpqrInvalidRequest, "shard host %q is not reachable: %v", h, err)
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
