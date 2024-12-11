package pool

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

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

type TsaKey struct {
	Tsa  tsa.TSA
	Host string
	AZ   string
}

type DBPool struct {
	Pool
	pool           MultiShardPool
	shardMapping   map[string]*config.Shard
	cacheTSAchecks sync.Map
	checker        tsa.TSAChecker

	ShuffleHosts bool
	PreferAZ     string
}

// ConnectionHost implements DBPool.
func (s *DBPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.Shard, error) {
	return s.pool.ConnectionHost(clid, shardKey, host)
}

// View implements DBPool.
// Subtle: this method shadows the method (Pool).View of InstancePoolImpl.Pool.
func (s *DBPool) View() Statistics {
	panic("unimplemented")
}

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
// TODO : unit tests
func (s *DBPool) traverseHostsMatchCB(clid uint, key kr.ShardKey, hosts []config.Host, cb func(shard.Shard) bool, tsa tsa.TSA) shard.Shard {
	for _, host := range hosts {
		sh, err := s.pool.ConnectionHost(clid, key, host)
		if err != nil {

			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  tsa,
				Host: host.Address,
				AZ:   host.AZ,
			}, false)

			spqrlog.Zero.Error().
				Err(err).
				Str("host", host.Address).
				Str("az", host.AZ).
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

// selectReadOnlyShardHost selects a read-only shard host from the given list of hosts based on the provided client ID and shard key.
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
// TODO : unit tests
func (s *DBPool) selectReadOnlyShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.Shard, error) {
	totalMsg := make([]string, 0)
	sh := s.traverseHostsMatchCB(clid, key, hosts, func(shard shard.Shard) bool {
		if ch, reason, err := s.checker.CheckTSA(shard); err != nil {
			totalMsg = append(totalMsg, fmt.Sprintf("host %s: ", shard.Instance().Hostname())+err.Error())
			_ = s.pool.Discard(shard)

			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  tsa,
				Host: shard.Instance().Hostname(),
				AZ:   shard.Instance().AvailabilityZone(),
			}, false)

			return false
		} else {
			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  tsa,
				Host: shard.Instance().Hostname(),
				AZ:   shard.Instance().AvailabilityZone(),
			}, !ch)

			if ch {
				totalMsg = append(totalMsg, fmt.Sprintf("host %s: read-only check fail: %s ", shard.Instance().Hostname(), reason))
				_ = s.Put(shard)
				return false
			}

			return true
		}
	}, tsa)
	if sh != nil {
		return sh, nil
	}

	return nil, fmt.Errorf("shard %s failed to find replica within %s", key.Name, strings.Join(totalMsg, ";"))
}

// selectReadWriteShardHost selects a read-write shard host from the given list of hosts based on the provided client ID and shard key.
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
// TODO : unit tests
func (s *DBPool) selectReadWriteShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.Shard, error) {
	totalMsg := make([]string, 0)
	sh := s.traverseHostsMatchCB(clid, key, hosts, func(shard shard.Shard) bool {
		if ch, reason, err := s.checker.CheckTSA(shard); err != nil {
			totalMsg = append(totalMsg, fmt.Sprintf("host %s: ", shard.Instance().Hostname())+err.Error())
			_ = s.pool.Discard(shard)

			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  tsa,
				Host: shard.Instance().Hostname(),
				AZ:   shard.Instance().AvailabilityZone(),
			}, false)

			return false
		} else {
			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  tsa,
				Host: shard.Instance().Hostname(),
				AZ:   shard.Instance().AvailabilityZone(),
			}, ch)

			if !ch {
				totalMsg = append(totalMsg, fmt.Sprintf("host %s: read-write check fail: %s ", shard.Instance().Hostname(), reason))
				_ = s.Put(shard)
				return false
			}

			return true
		}
	}, tsa)
	if sh != nil {
		return sh, nil
	}

	return nil, fmt.Errorf("shard %s failed to find primary within %s", key.Name, strings.Join(totalMsg, ";"))
}

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
// TODO : unit tests
func (s *DBPool) ConnectionWithTSA(clid uint, key kr.ShardKey, targetSessionAttrs tsa.TSA) (shard.Shard, error) {
	spqrlog.Zero.Debug().
		Uint("client", clid).
		Str("shard", key.Name).
		Str("tsa", string(targetSessionAttrs)).
		Msg("acquiring new instance connection for client to shard with target session attrs")

	hostOrder, err := s.BuildHostOrder(key, targetSessionAttrs)
	if err != nil {
		return nil, err
	}

	/* pool.Connection will reoder hosts in such way, that preferred tsa will go first */
	switch targetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:
		total_msg := make([]string, 0)
		for _, host := range hostOrder {
			shard, err := s.pool.ConnectionHost(clid, key, host)
			if err != nil {
				total_msg = append(total_msg, fmt.Sprintf("host %s: %s", host, err.Error()))

				s.cacheTSAchecks.Store(TsaKey{
					Tsa:  config.TargetSessionAttrsAny,
					Host: host.Address,
					AZ:   host.AZ,
				}, false)

				spqrlog.Zero.Error().
					Err(err).
					Str("host", host.Address).
					Str("availability-zone", host.AZ).
					Uint("client", clid).
					Msg("failed to get connection to host for client")
				continue
			}
			s.cacheTSAchecks.Store(TsaKey{
				Tsa:  config.TargetSessionAttrsAny,
				Host: host.Address,
				AZ:   host.AZ,
			}, true)

			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within: %s", strings.Join(total_msg, ", "))
	case config.TargetSessionAttrsRO:
		return s.selectReadOnlyShardHost(clid, key, hostOrder, targetSessionAttrs)
	case config.TargetSessionAttrsPS:
		if res, err := s.selectReadOnlyShardHost(clid, key, hostOrder, targetSessionAttrs); err != nil {
			return s.selectReadWriteShardHost(clid, key, hostOrder, targetSessionAttrs)
		} else {
			return res, nil
		}
	case config.TargetSessionAttrsRW:
		return s.selectReadWriteShardHost(clid, key, hostOrder, targetSessionAttrs)
	default:
		return nil, fmt.Errorf("failed to match correct target session attrs")
	}
}

func (s *DBPool) BuildHostOrder(key kr.ShardKey, targetSessionAttrs tsa.TSA) ([]config.Host, error) {
	var hostOrder []config.Host
	var posCache []config.Host
	var negCache []config.Host

	if _, ok := s.shardMapping[key.Name]; !ok {
		return nil, fmt.Errorf("shard with name %q not found", key.Name)
	}

	for _, host := range s.shardMapping[key.Name].HostsAZ() {
		tsaKey := TsaKey{
			Tsa:  targetSessionAttrs,
			Host: host.Address,
			AZ:   host.AZ,
		}

		if res, ok := s.cacheTSAchecks.Load(tsaKey); ok {
			if res.(bool) {
				posCache = append(posCache, host)
			} else {
				negCache = append(negCache, host)
			}
		} else {

			posCache = append(posCache, host)
		}
	}

	if s.ShuffleHosts {
		rand.Shuffle(len(posCache), func(i, j int) {
			posCache[i], posCache[j] = posCache[j], posCache[i]
		})
		rand.Shuffle(len(negCache), func(i, j int) {
			negCache[i], negCache[j] = negCache[j], negCache[i]
		})
	}

	if len(s.PreferAZ) > 0 {
		sort.Slice(posCache, func(i, j int) bool {
			return posCache[i].AZ == s.PreferAZ
		})
		sort.Slice(negCache, func(i, j int) bool {
			return negCache[i].AZ == s.PreferAZ
		})
	}

	hostOrder = append(posCache, negCache...)
	return hostOrder, nil
}

// SetRule initializes the backend rule in the instance pool.
// It takes a pointer to a BackendRule as a parameter and saves it
//
// Parameters:
//   - rule: A pointer to a BackendRule representing the backend rule to be initialized.
func (s *DBPool) SetRule(rule *config.BackendRule) {
	s.pool.SetRule(rule)
}

// ShardMapping returns the shard mapping of the instance pool.
//
// Returns:
//   - map[string]*config.Shard: The shard mapping of the instance pool.
func (s *DBPool) ShardMapping() map[string]*config.Shard {
	return s.shardMapping
}

// ForEach iterates over each shard in the instance pool and calls the provided callback function.
// It returns an error if the callback function returns an error.
//
// Parameters:
// - cb: The callback function to be called for each shard in the instance pool.
//
// Returns:
// - error: An error if the callback function returns an error.
func (s *DBPool) ForEach(cb func(sh shard.Shardinfo) error) error {
	return s.pool.ForEach(cb)
}

// Put puts a shard into the instance pool.
// It discards the shard if it is not synchronized or if it is not in idle transaction status.
// Otherwise, it puts the shard into the pool.
//
// Parameters:
// - sh: The shard to be put into the pool.
//
// Returns:
// - error: An error if the shard is discarded or if there is an error putting the shard into the pool.
//
// TODO : unit tests
func (s *DBPool) Put(sh shard.Shard) error {
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
func (s *DBPool) ForEachPool(cb func(pool Pool) error) error {
	return s.pool.ForEachPool(cb)
}

// Discard removes a shard from the instance pool.
// It returns an error if the removal fails.
//
// Parameters:
// - sh: The shard to be removed from the pool.
//
// Returns:
// - error: An error if the removal fails, nil otherwise.
func (s *DBPool) Discard(sh shard.Shard) error {
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
func NewDBPool(mapping map[string]*config.Shard, startupParams *startup.StartupParams, preferAZ string) *DBPool {
	allocator := func(shardKey kr.ShardKey, host config.Host, rule *config.BackendRule) (shard.Shard, error) {
		shardConfig := mapping[shardKey.Name]
		hostname, _, _ := net.SplitHostPort(host.Address) // TODO try to remove this
		tlsconfig, err := shardConfig.TLS.Init(hostname)
		if err != nil {
			return nil, err
		}

		connTimeout := config.ValueOrDefaultDuration(rule.ConnectionTimeout, defaultInstanceConnectionTimeout)
		keepAlive := config.ValueOrDefaultDuration(rule.KeepAlive, defaultKeepAlive)
		tcpUserTimeout := config.ValueOrDefaultDuration(rule.TcpUserTimeout, defaultTcpUserTimeout)

		pgi, err := conn.NewInstanceConn(host.Address, host.AZ, shardKey.Name, tlsconfig, connTimeout, keepAlive, tcpUserTimeout)
		if err != nil {
			return nil, err
		}

		return datashard.NewShard(shardKey, pgi, mapping[shardKey.Name], rule, startupParams)
	}

	return &DBPool{
		pool:           NewPool(allocator),
		shardMapping:   mapping,
		ShuffleHosts:   true,
		PreferAZ:       preferAZ,
		cacheTSAchecks: sync.Map{},
		checker:        tsa.NewTSAChecker(),
	}
}

func NewDBPoolFromMultiPool(mapping map[string]*config.Shard, sp *startup.StartupParams, mp MultiShardPool, tsaRecheckDuration time.Duration) *DBPool {
	return &DBPool{
		pool:           mp,
		shardMapping:   mapping,
		cacheTSAchecks: sync.Map{},
		checker:        tsa.NewTSACheckerWithDuration(tsaRecheckDuration),
	}
}
