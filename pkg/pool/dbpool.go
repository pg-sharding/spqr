package pool

import (
	"fmt"
	"math/rand"
	"net"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type InstancePoolImpl struct {
	Pool
	pool         MultiShardPool
	shardMapping map[string]*config.Shard
}

var _ DBPool = &InstancePoolImpl{}

func (s *InstancePoolImpl) Connection(
	clid string,
	key kr.ShardKey,
	TargetSessionAttrs string) (shard.Shard, error) {
	// spqrlog.Logger.Printf(spqrlog.DEBUG1, "acquiring new instance connection for client '%s' to shard '%s' with tsa: '%s'", clid, key.Name, TargetSessionAttrs)

	hosts := make([]string, len(s.shardMapping[key.Name].Hosts))
	copy(hosts, s.shardMapping[key.Name].Hosts)
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[j], hosts[i] = hosts[i], hosts[j]
	})

	switch TargetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:
		total_msg := ""
		for _, host := range hosts {
			shard, err := s.pool.Connection(clid, key, host)
			if err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()
				// spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v", host, clid, err)
				continue
			}
			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within %s", total_msg)
	case config.TargetSessionAttrsRO:
		total_msg := ""

		for _, host := range hosts {
			shard, err := s.pool.Connection(clid, key, host)
			if err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()
				// spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v ", host, clid, err)
				continue
			}
			if ch, reason, err := tsa.CheckTSA(shard); err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()
				_ = s.pool.Discard(shard)
				continue
			} else if ch {
				total_msg += fmt.Sprintf("host %s: read-only check fail: %s ", host, reason)
				_ = s.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("shard %s failed to find replica within %s", key.Name, total_msg)
	case config.TargetSessionAttrsRW:
		total_msg := ""
		for _, host := range hosts {
			shard, err := s.pool.Connection(clid, key, host)
			if err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()
				// spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v ", host, clid, err)
				continue
			}
			if ch, reason, err := tsa.CheckTSA(shard); err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()
				_ = s.pool.Discard(shard)
				continue
			} else if !ch {
				total_msg += fmt.Sprintf("host %s: read-write check fail: %s ", host, reason)
				_ = s.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("shard %s failed to find primary within %s", key.Name, total_msg)
	default:
		return nil, fmt.Errorf("failed to match correct target session attrs")
	}
}

func (s *InstancePoolImpl) InitRule(rule *config.BackendRule) error {
	return s.pool.InitRule(rule)
}

func (s *InstancePoolImpl) ShardMapping() map[string]*config.Shard {
	return s.shardMapping
}

func (s *InstancePoolImpl) List() []shard.Shard {
	/* mutex? */
	return s.pool.List()
}

func (s *InstancePoolImpl) ForEach(cb func(sh shard.Shard) error) error {
	return s.pool.ForEach(cb)
}

func (s *InstancePoolImpl) Put(sh shard.Shard) error {
	if sh.Sync() != 0 {
		// spqrlog.Logger.Printf(spqrlog.ERROR, "discarding unsync connection %p, sync %d", sh, sh.Sync())
		return s.pool.Discard(sh)
	}
	return s.pool.Put(sh)
}

func (s *InstancePoolImpl) ForEachPool(cb func(pool Pool) error) error {
	return s.pool.ForEachPool(cb)
}

func (s *InstancePoolImpl) Cut(host string) []shard.Shard {
	return s.pool.Cut(host)
}

func (s *InstancePoolImpl) Discard(sh shard.Shard) error {
	return s.pool.Discard(sh)
}

func NewDBPool(mapping map[string]*config.Shard) DBPool {
	allocator := func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		shard := mapping[shardKey.Name]

		addr, _, _ := net.SplitHostPort(host)
		tlsconfig, err := shard.TLS.Init(addr)
		if err != nil {
			return nil, err
		}
		pgi, err := conn.NewInstanceConn(host, tlsconfig)
		if err != nil {
			return nil, err
		}
		shardC, err := datashard.NewShard(shardKey, pgi, mapping[shardKey.Name], rule)
		if err != nil {
			return nil, err
		}
		return shardC, nil
	}

	return &InstancePoolImpl{
		pool:         NewPool(allocator),
		shardMapping: mapping,
	}
}
