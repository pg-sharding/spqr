package topology

import (
	"context"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

const defaultShardHostValidationTimeout = 3 * time.Second

func ValidateDataShardHosts(ctx context.Context, shard *DataShard) error {
	if shard == nil {
		return spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "shard definition is nil")
	}
	if shard.Cfg == nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard %q has no config", shard.ID)
	}

	hosts := shard.Cfg.Hosts()
	if len(shard.Cfg.RawHosts) != 0 && len(hosts) != len(shard.Cfg.RawHosts) {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard %q has invalid or unsupported host definitions", shard.ID)
	}
	if len(hosts) == 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard %q has no valid hosts configured", shard.ID)
	}

	dialer := &net.Dialer{Timeout: defaultShardHostValidationTimeout}
	for _, host := range hosts {
		conn, err := dialer.DialContext(ctx, "tcp", host)
		if err != nil {
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "shard host %q is not reachable: %v", host, err)
		}
		if err := conn.Close(); err != nil {
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "failed to close validation connection to shard host %q: %v", host, err)
		}
	}

	return nil
}
