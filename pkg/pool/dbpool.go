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
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type InstancePoolImpl struct {
	Pool
	pool         MultiShardPool
	shardMapping map[string]*config.Shard

	checker tsa.TSAChecker
}

var _ DBPool = &InstancePoolImpl{}

// TODO : unit tests
// traverseHostsMatchCB traverses the list of hosts and invokes the provided callback function
// for each host until the callback returns true. It returns the shard that satisfies the callback
// condition. If no shard satisfies the condition, it returns nil.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//   - cb: The callback function that takes a shard and returns a boolean value.
//
// Returns:
//   - shard.Shard: The shard that satisfies the callback condition, or nil if no shard satisfies the condition.
//
// traverseHostsMatchCB traverses the list of hosts and invokes the provided callback function
// for each host until the callback returns true. It returns the shard that satisfies the callback
// condition. If no shard satisfies the condition, it returns nil.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//   - cb: The callback function that takes a shard and returns a boolean value.
//
// Returns:
//   - shard.Shard: The shard that satisfies the callback condition, or nil if no shard satisfies the condition.
func (s *InstancePoolImpl) traverseHostsMatchCB(
	clid uint,
	key kr.ShardKey, hosts []string, cb func(shard.Shard) bool) shard.Shard {

	for _, host := range hosts {
		sh, err := s.pool.Connection(clid, key, host)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("host", host).
				Uint("client", clid).
				Msg("failed to get connection to host for client")
			continue
		}

		// callback should Discard/Put connection to Pool properly here
		if !cb(sh) {
			continue
		}

		return sh
	}

	return nil
}

// TODO : unit tests
// SelectReadOnlyShardHost selects a read-only shard host from the given list of hosts based on the provided client ID and shard key.
// It traverses the hosts and performs checks to ensure the selected shard host is suitable for read-only operations.
// If a suitable shard host is found, it is returned along with a nil error.
// If no suitable shard host is found, an error is returned with a message indicating the reason for failure.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//
// Returns:
//   - shard.Shard: The selected read-only shard host.
//   - error: An error if no suitable shard host is found.
//
// SelectReadOnlyShardHost selects a read-only shard host from the given list of hosts based on the provided client ID and shard key.
// It traverses the hosts and performs checks to ensure the selected shard host is suitable for read-only operations.
// If a suitable shard host is found, it is returned along with a nil error.
// If no suitable shard host is found, an error is returned with a message indicating the reason for failure.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//
// Returns:
//   - shard.Shard: The selected read-only shard host.
//   - error: An error if no suitable shard host is found.
func (s *InstancePoolImpl) SelectReadOnlyShardHost(
	clid uint,
	key kr.ShardKey, hosts []string) (shard.Shard, error) {
	total_msg := ""
	sh := s.traverseHostsMatchCB(clid, key, hosts, func(shard shard.Shard) bool {
		if ch, reason, err := s.checker.CheckTSA(shard); err != nil {
			total_msg += fmt.Sprintf("host %s: ", shard.Instance().Hostname()) + err.Error()
			_ = s.pool.Discard(shard)
			return false
		} else if ch {
			total_msg += fmt.Sprintf("host %s: read-only check fail: %s ", shard.Instance().Hostname(), reason)
			_ = s.Put(shard)
			return false
		}
		return true
	})
	if sh != nil {
		return sh, nil
	}

	return nil, fmt.Errorf("shard %s failed to find replica within %s", key.Name, total_msg)
}

// TODO : unit tests
// SelectReadWriteShardHost selects a read-write shard host from the given list of hosts based on the provided client ID and shard key.
// It traverses the hosts and checks if each shard is available and suitable for read-write operations.
// If a suitable shard is found, it is returned along with no error.
// If no suitable shard is found, an error is returned indicating the failure reason.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//
// Returns:
//   - shard.Shard: The selected read-write shard host.
//   - error: An error if no suitable shard host is found.
//
// SelectReadWriteShardHost selects a read-write shard host from the given list of hosts based on the provided client ID and shard key.
// It traverses the hosts and checks if each shard is available and suitable for read-write operations.
// If a suitable shard is found, it is returned along with no error.
// If no suitable shard is found, an error is returned indicating the failure reason.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//
// Returns:
//   - shard.Shard: The selected read-write shard host.
//   - error: An error if no suitable shard host is found.
func (s *InstancePoolImpl) SelectReadWriteShardHost(
	clid uint,
	key kr.ShardKey, hosts []string) (shard.Shard, error) {

	total_msg := ""
	sh := s.traverseHostsMatchCB(clid, key, hosts, func(shard shard.Shard) bool {

		if ch, reason, err := s.checker.CheckTSA(shard); err != nil {
			total_msg += fmt.Sprintf("host %s: ", shard.Instance().Hostname()) + err.Error()
			_ = s.pool.Discard(shard)
			return false
		} else if !ch {
			total_msg += fmt.Sprintf("host %s: read-write check fail: %s ", shard.Instance().Hostname(), reason)
			_ = s.Put(shard)
			return false
		}
		return true
	})
	if sh != nil {
		return sh, nil
	}

	return nil, fmt.Errorf("shard %s failed to find primary within %s", key.Name, total_msg)
}

// TODO : unit tests
// Connection acquires a new instance connection for a client to a shard with target session attributes.
// It selects a shard host based on the target session attributes and returns a shard connection.
// If no connection can be established, it returns an error.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - targetSessionAttrs: The target session attributes.
//
// Returns:
//   - shard.Shard: The acquired shard connection.
//   - error: An error if the connection cannot be established.
//
// Connection acquires a new instance connection for a client to a shard with target session attributes.
// It selects a shard host based on the target session attributes and returns a shard connection.
// If no connection can be established, it returns an error.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - targetSessionAttrs: The target session attributes.
//
// Returns:
//   - shard.Shard: The acquired shard connection.
//   - error: An error if the connection cannot be established.
func (s *InstancePoolImpl) Connection(
	clid uint,
	key kr.ShardKey,
	targetSessionAttrs string) (shard.Shard, error) {
	spqrlog.Zero.Debug().
		Uint("client", clid).
		Str("shard", key.Name).
		Str("tsa", targetSessionAttrs).
		Msg("acquiring new instance connection for client to shard with target session attrs")
	hosts := make([]string, len(s.shardMapping[key.Name].Hosts))
	copy(hosts, s.shardMapping[key.Name].Hosts)
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[j], hosts[i] = hosts[i], hosts[j]
	})

	switch targetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:
		total_msg := ""
		for _, host := range hosts {
			shard, err := s.pool.Connection(clid, key, host)
			if err != nil {
				total_msg += fmt.Sprintf("host %s: ", host) + err.Error()

				spqrlog.Zero.Error().
					Err(err).
					Str("host", host).
					Uint("client", clid).
					Msg("failed to get connection to host for client")
				continue
			}
			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within %s", total_msg)
	case config.TargetSessionAttrsRO:
		return s.SelectReadOnlyShardHost(clid, key, hosts)
	case config.TargetSessionAttrsPS:
		if res, err := s.SelectReadOnlyShardHost(clid, key, hosts); err != nil {
			return s.SelectReadWriteShardHost(clid, key, hosts)
		} else {
			return res, nil
		}
	case config.TargetSessionAttrsRW:
		return s.SelectReadWriteShardHost(clid, key, hosts)
	default:
		return nil, fmt.Errorf("failed to match correct target session attrs")
	}
}

// InitRule initializes the backend rule in the instance pool.
// It takes a pointer to a BackendRule as a parameter and returns an error.
//
// Parameters:
//   - rule: A pointer to a BackendRule representing the backend rule to be initialized.
//
// Returns:
//   - error: An error if there is an error initializing the backend rule, nil otherwise.
//
// InitRule initializes the backend rule in the instance pool.
// It takes a pointer to a BackendRule as a parameter and returns an error.
//
// Parameters:
//   - rule: A pointer to a BackendRule representing the backend rule to be initialized.
//
// Returns:
//   - error: An error if there is an error initializing the backend rule, nil otherwise.
func (s *InstancePoolImpl) InitRule(rule *config.BackendRule) error {
	return s.pool.InitRule(rule)
}

// ShardMapping returns the shard mapping of the instance pool.
// ShardMapping returns the shard mapping of the instance pool.
func (s *InstancePoolImpl) ShardMapping() map[string]*config.Shard {
	return s.shardMapping
}

// List returns a list of shards in the instance pool.
// List returns a list of shards in the instance pool.
func (s *InstancePoolImpl) List() []shard.Shard {
	/* mutex? */
	return s.pool.List()
}

// ForEach iterates over each shard in the instance pool and calls the provided callback function.
// It returns an error if the callback function returns an error.
//
// Parameters:
// - cb: The callback function to be called for each shard in the instance pool.
//
// Returns:
// - error: An error if the callback function returns an error.
// ForEach iterates over each shard in the instance pool and calls the provided callback function.
// It returns an error if the callback function returns an error.
//
// Parameters:
// - cb: The callback function to be called for each shard in the instance pool.
//
// Returns:
// - error: An error if the callback function returns an error.
func (s *InstancePoolImpl) ForEach(cb func(sh shard.Shardinfo) error) error {
	return s.pool.ForEach(cb)
}

// TODO : unit tests
// Put puts a shard into the instance pool.
// It discards the shard if it is not synchronized or if it is not in idle transaction status.
// Otherwise, it puts the shard into the pool.
//
// Parameters:
// - sh: The shard to be put into the pool.
//
// Returns:
// - error: An error if the shard is discarded or if there is an error putting the shard into the pool.
// Put puts a shard into the instance pool.
// It discards the shard if it is not synchronized or if it is not in idle transaction status.
// Otherwise, it puts the shard into the pool.
//
// Parameters:
// - sh: The shard to be put into the pool.
//
// Returns:
// - error: An error if the shard is discarded or if there is an error putting the shard into the pool.
func (s *InstancePoolImpl) Put(sh shard.Shard) error {
	if sh.Sync() != 0 {
		spqrlog.Zero.Error().
			Uint("shard", spqrlog.GetPointer(sh)).
			Int64("sync", sh.Sync()).
			Msg("discarding unsync connection")
		return s.pool.Discard(sh)
	}
	if sh.TxStatus() != txstatus.TXIDLE {
		spqrlog.Zero.Error().
			Uint("shard", spqrlog.GetPointer(sh)).
			Str("txstatus", sh.TxStatus().String()).
			Msg("discarding non-idle connection")
		return s.pool.Discard(sh)
	}
	return s.pool.Put(sh)
}

// ForEachPool iterates over each pool in the instance pool and calls the provided callback function.
// It returns an error if the callback function returns an error.
//
// Parameters:
// - cb: The callback function to be called for each pool in the instance pool.
//
// Returns:
// - error: An error if the callback function returns an error.
// ForEachPool iterates over each pool in the instance pool and calls the provided callback function.
// It returns an error if the callback function returns an error.
//
// Parameters:
// - cb: The callback function to be called for each pool in the instance pool.
//
// Returns:
// - error: An error if the callback function returns an error.
func (s *InstancePoolImpl) ForEachPool(cb func(pool Pool) error) error {
	return s.pool.ForEachPool(cb)
}

// Cut removes a shard from the instance pool based on the provided host.
// It returns the removed shard.
//
// Parameters:
// - host: The host of the shard to be removed.
//
// Returns:
// - []shard.Shard: The removed shard.
// Cut removes a shard from the instance pool based on the provided host.
// It returns the removed shard.
//
// Parameters:
// - host: The host of the shard to be removed.
//
// Returns:
// - []shard.Shard: The removed shard.
func (s *InstancePoolImpl) Cut(host string) []shard.Shard {
	return s.pool.Cut(host)
}

// Discard removes a shard from the instance pool.
// It returns an error if the removal fails.
//
// Parameters:
// - sh: The shard to be removed from the pool.
//
// Returns:
// - error: An error if the removal fails, nil otherwise.
// Discard removes a shard from the instance pool.
// It returns an error if the removal fails.
//
// Parameters:
// - sh: The shard to be removed from the pool.
//
// Returns:
// - error: An error if the removal fails, nil otherwise.
func (s *InstancePoolImpl) Discard(sh shard.Shard) error {
	return s.pool.Discard(sh)
}

// NewDBPool creates a new DBPool instance with the given shard mapping.
// It uses the provided mapping to allocate shards based on the shard key,
// and initializes the necessary connections and configurations for each shard.
// The function returns a DBPool interface that can be used to interact with the pool.
//
// Parameters:
//   - mapping: A map containing the shard mapping, where the key is the shard name
//     and the value is a pointer to the corresponding Shard configuration.
//   - sp: A StartupParams
//
// Returns:
//   - DBPool: A DBPool interface that represents the created pool.
func NewDBPool(mapping map[string]*config.Shard, sp *startup.StartupParams) DBPool {
	allocator := func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		shard := mapping[shardKey.Name]

		addr, _, _ := net.SplitHostPort(host)
		tlsconfig, err := shard.TLS.Init(addr)
		if err != nil {
			return nil, err
		}
		pgi, err := conn.NewInstanceConn(host, shardKey.Name, tlsconfig)
		if err != nil {
			return nil, err
		}
		shardC, err := datashard.NewShard(shardKey, pgi, mapping[shardKey.Name], rule, sp)
		if err != nil {
			return nil, err
		}
		return shardC, nil
	}

	return &InstancePoolImpl{
		pool:         NewPool(allocator),
		shardMapping: mapping,
		checker:      tsa.NewTSAChecker(),
	}
}
