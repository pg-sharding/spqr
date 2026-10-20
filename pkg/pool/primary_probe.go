package pool

import (
	"context"
	"errors"
	"net"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

// probePrimary uses a fresh connection independent of application pool limits.
// ctx carries the single deadline assigned when the scheduler starts the probe.
func probePrimary(ctx context.Context, tmgr topology.TopologyMgr, startupParams *startup.StartupParams, rule *config.BackendRule, key kr.ShardKey, host config.Host) (tsa.CheckResult, string, error) {
	failure := func(stage string, err error) (tsa.CheckResult, string, error) {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return tsa.CheckResult{Reason: err.Error()}, stage, err
	}
	shardConfig, err := tmgr.ShardById(key.Name)
	if err != nil {
		return failure("configuration", err)
	}
	if rule == nil {
		return failure("configuration", errors.New("backend rule is not installed"))
	}
	hostname, _, err := net.SplitHostPort(host.Address)
	if err != nil {
		return failure("connect", err)
	}
	tlsconfig, err := shardConfig.TLS().Init(hostname)
	if err != nil {
		return failure("tls", err)
	}
	pgi, err := conn.NewInstanceConnContext(ctx, host.Address, host.AZ, key.Name, tlsconfig,
		config.ValueOrDefaultDuration(rule.KeepAlive, defaultKeepAlive),
		config.ValueOrDefaultDuration(rule.TCPUserTimeout, defaultTCPUserTimeout))
	if err != nil {
		stage := "connect"
		var connectionError *conn.ConnectionError
		if errors.As(err, &connectionError) {
			stage = connectionError.Stage
		}
		return failure(stage, err)
	}
	defer func() { _ = pgi.Close() }()
	if startupParams == nil {
		startupParams = &startup.StartupParams{}
	}
	sh, err := datashard.NewShardHostInstance(key, pgi, shardConfig, rule, startupParams)
	if err != nil {
		return failure("authentication", err)
	}
	result, err := (tsa.NetChecker{}).CheckTSA(sh, 0)
	if err != nil {
		return failure("query", err)
	}
	if err := ctx.Err(); err != nil {
		return failure("query", err)
	}
	return result, "", nil
}
