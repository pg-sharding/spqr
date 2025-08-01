package pool

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strings"
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

type LocalCheckResult struct {
	Alive  bool
	Match  bool
	Reason string
}

type DBPool struct {
	pool         ShardHostsPool
	shardMapping map[string]*config.Shard
	checker      *tsa.CachedTSAChecker

	cache        *DbpoolCache
	ShuffleHosts bool
	PreferAZ     string
}

// InstanceHealthChecks implements MultiShardTSAPool.
func (s *DBPool) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return s.checker.InstanceHealthChecks()
}

// View implements MultiShardPool.
func (s *DBPool) View() Statistics {
	return s.pool.View()
}

// ID implements MultiShardPool.
func (s *DBPool) ID() uint {
	return spqrlog.GetPointer(s)
}

// ConnectionHost implements DBPool.
func (s *DBPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error) {
	return s.pool.ConnectionHost(clid, shardKey, host)
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
func (s *DBPool) traverseHostsMatchCB(clid uint, key kr.ShardKey, hosts []config.Host, cb func(shard.ShardHostInstance) bool, tsa tsa.TSA) shard.ShardHostInstance {
	for _, host := range hosts {
		sh, err := s.pool.ConnectionHost(clid, key, host)
		if err != nil {

			s.cache.MarkUnmatched(tsa, host.Address, host.AZ, false, err.Error())

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

// selectReadOnlyShardHost wraps the selectShardHost method to specifically select a read-only shard host.
func (s *DBPool) selectReadOnlyShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.ShardHostInstance, error) {
	return s.selectShardHost(clid, key, hosts, tsa, false)
}

// selectReadWriteShardHost wraps the selectShardHost method to specifically select a read-write shard host.
func (s *DBPool) selectReadWriteShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.ShardHostInstance, error) {
	return s.selectShardHost(clid, key, hosts, tsa, true)
}

// selectShardHost selects a shard host based on the provided client ID, shard key, list of hosts,
// target session attributes, and whether the host should be a primary or not.
// It traverses the hosts and checks if they are suitable for the given shard key and target
// session attributes. If a suitable host is found, it returns the shard; otherwise, it returns an error.
//
// Parameters:
//   - clid: The client ID.
//   - key: The shard key.
//   - hosts: The list of hosts to traverse.
//   - tsa: The target session attributes.
//   - primary: A boolean indicating whether the host should be a primary or not.
//
// Returns:
//   - shard.Shard: The selected shard host.
//   - error: An error if no suitable host is found or if there is an issue
//     during the selection process.
//
// // TODO : unit tests
func (s *DBPool) selectShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA, primary bool) (shard.ShardHostInstance, error) {
	hostToReason := map[string]string{}
	sh := s.traverseHostsMatchCB(clid, key, hosts, func(shard shard.ShardHostInstance) bool {
		tcr, err := s.checker.CheckTSA(shard)
		good := tcr.CR.RW == primary

		if err != nil {
			hostToReason[shard.Instance().Hostname()] = err.Error()
			_ = s.pool.Discard(shard)

			s.cache.MarkUnmatched(tsa, shard.Instance().Hostname(), shard.Instance().AvailabilityZone(), tcr.CR.Alive, err.Error())

			return false
		}

		if good {
			s.cache.MarkMatched(tsa, shard.Instance().Hostname(), shard.Instance().AvailabilityZone(), tcr.CR.Alive, tcr.CR.Reason)
		} else {
			s.cache.MarkUnmatched(tsa, shard.Instance().Hostname(), shard.Instance().AvailabilityZone(), tcr.CR.Alive, tcr.CR.Reason)
		}

		if tcr.CR.Alive && good {
			return true
		}

		// Host is not suitable
		hostToReason[shard.Instance().Hostname()] = tcr.CR.Reason
		_ = s.Put(shard)
		return false

	}, tsa)
	if sh != nil {
		return sh, nil
	}

	messages := make([]string, 0, len(hostToReason))
	for host, reason := range hostToReason {
		messages = append(messages, fmt.Sprintf("host %s: %s", host, reason))
	}

	roleType := "replica"
	if primary {
		roleType = "primary"
	}
	return nil, fmt.Errorf("shard %s: failed to find %s within %s", key.Name, roleType, strings.Join(messages, ";"))
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
func (s *DBPool) ConnectionWithTSA(clid uint, key kr.ShardKey, targetSessionAttrs tsa.TSA) (shard.ShardHostInstance, error) {

	var effectiveTargetSessionAttrs tsa.TSA
	if targetSessionAttrs == config.TargetSessionAttrsSmartRW {
		if key.RO {
			/* if query is proved read-only, try to pick up a standby */
			effectiveTargetSessionAttrs = config.TargetSessionAttrsPS
		} else {
			effectiveTargetSessionAttrs = config.TargetSessionAttrsRW
		}
	} else {
		effectiveTargetSessionAttrs = targetSessionAttrs
	}

	spqrlog.Zero.Debug().
		Uint("client", clid).
		Str("shard", key.Name).
		Bool("RO", key.RO).
		Str("effective tsa", string(effectiveTargetSessionAttrs)).
		Msg("acquiring new instance connection for client to shard with target session attrs")

	hostOrder, err := s.BuildHostOrder(key, effectiveTargetSessionAttrs)
	if err != nil {
		return nil, err
	}

	/* pool.Connection will reorder hosts in such way, that preferred tsa will go first */
	switch effectiveTargetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:
		total_msg := make([]string, 0)
		for _, host := range hostOrder {
			shard, err := s.pool.ConnectionHost(clid, key, host)
			if err != nil {
				total_msg = append(total_msg, fmt.Sprintf("host %s: %s", host, err.Error()))

				s.cache.MarkUnmatched(config.TargetSessionAttrsAny, host.Address, host.AZ, false, err.Error())

				spqrlog.Zero.Error().
					Err(err).
					Str("host", host.Address).
					Str("availability-zone", host.AZ).
					Uint("client", clid).
					Msg("failed to get connection to host for client")
				continue
			}

			s.cache.MarkMatched(config.TargetSessionAttrsAny, host.Address, host.AZ, true, "target session attrs any")

			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within: %s", strings.Join(total_msg, ", "))
	case config.TargetSessionAttrsRO:
		return s.selectReadOnlyShardHost(clid, key, hostOrder, effectiveTargetSessionAttrs)
	case config.TargetSessionAttrsPS:
		fallthrough
	case config.TargetSessionAttrsPR:
		if res, err := s.selectReadOnlyShardHost(clid, key, hostOrder, effectiveTargetSessionAttrs); err != nil {
			return s.selectReadWriteShardHost(clid, key, hostOrder, effectiveTargetSessionAttrs)
		} else {
			return res, nil
		}
	case config.TargetSessionAttrsRW:
		return s.selectReadWriteShardHost(clid, key, hostOrder, effectiveTargetSessionAttrs)
	default:
		return nil, fmt.Errorf("failed to match correct target session attrs")
	}
}

func (s *DBPool) BuildHostOrder(key kr.ShardKey, targetSessionAttrs tsa.TSA) ([]config.Host, error) {
	var hostOrder []config.Host
	var posCache []config.Host
	var negCache []config.Host
	var deadCache []config.Host

	if _, ok := s.shardMapping[key.Name]; !ok {
		return nil, fmt.Errorf("shard with name %q not found", key.Name)
	}

	for _, host := range s.shardMapping[key.Name].HostsAZ() {
		cr, ok := s.cache.Match(targetSessionAttrs, host.Address, host.AZ)
		if ok {
			if !cr.Alive {
				deadCache = append(deadCache, host)
			} else if cr.Match {
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
		rand.Shuffle(len(deadCache), func(i, j int) {
			deadCache[i], deadCache[j] = deadCache[j], deadCache[i]
		})
	}

	if len(s.PreferAZ) > 0 {
		sort.Slice(posCache, func(i, j int) bool {
			return posCache[i].AZ == s.PreferAZ
		})
		sort.Slice(negCache, func(i, j int) bool {
			return negCache[i].AZ == s.PreferAZ
		})
		sort.Slice(deadCache, func(i, j int) bool {
			return deadCache[i].AZ == s.PreferAZ
		})
	}

	hostOrder = append(posCache, negCache...)
	hostOrder = append(hostOrder, deadCache...)
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
func (s *DBPool) ForEach(cb func(sh shard.ShardHostCtl) error) error {
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
func (s *DBPool) Put(sh shard.ShardHostInstance) error {
	if sh.Sync() != 0 || sh.IsStale() {
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
func (s *DBPool) Discard(sh shard.ShardHostInstance) error {
	return s.pool.Discard(sh)
}

// Cache returns the cache instance for testing purposes
func (s *DBPool) Cache() *DbpoolCache {
	return s.cache
}

// StopCacheWatchdog stops the cache cleanup goroutine
func (s *DBPool) StopCacheWatchdog() {
	s.cache.StopWatchdog()
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
func NewDBPool(mapping map[string]*config.Shard, startupParams *startup.StartupParams, preferAZ string, hostCheckInterval time.Duration) MultiShardTSAPool {
	allocator := func(shardKey kr.ShardKey, host config.Host, rule *config.BackendRule) (shard.ShardHostInstance, error) {
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

		return datashard.NewShardHostInstance(shardKey, pgi, mapping[shardKey.Name], rule, startupParams)
	}

	dbPool := &DBPool{
		pool:         NewPool(allocator),
		shardMapping: mapping,
		ShuffleHosts: true,
		PreferAZ:     preferAZ,
		checker:      tsa.NewCachedTSAChecker(),
	}

	// Create cache with cleanup functionality (5 minute max age)
	/* XXX: take PulseCheckInterval from config */
	dbPool.cache = NewDbpoolCacheWithCleanup(defaultCacheTTL, hostCheckInterval)

	return dbPool
}
