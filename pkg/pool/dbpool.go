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

type DBPool struct {
	pool         MultiShardPool
	shardMapping map[string]*config.Shard
	checker      *tsa.CachedTSAChecker

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
	panic("DBPool.View not implemented")
}

func (s *DBPool) selectShardHost(clid uint, key kr.ShardKey, hosts []config.Host, rw bool) (shard.Shard, error) {
	totalMsg := make([]string, 0)
	for _, host := range hosts {
		sh, err := s.pool.ConnectionHost(clid, key, host)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("host", host.Address).
				Str("az", host.AZ).
				Uint("client", clid).
				Msg("failed to get connection to host for client")

			totalMsg = append(totalMsg, fmt.Sprintf("host %s: %s", host.Address, err.Error()))
			continue
		}
		cr, err := s.checker.CheckTSA(sh)
		if err != nil {
			totalMsg = append(totalMsg, fmt.Sprintf("host %s: ", sh.Instance().Hostname())+err.Error())
			_ = s.pool.Discard(sh)
			continue
		}

		if !cr.Alive || cr.RW != rw {
			// TODO make shorter
			totalMsg = append(totalMsg, fmt.Sprintf("host %s is not suitable: alive=%t, rw=%t(need %t), reason: %s", sh.Instance().Hostname(), cr.Alive, cr.RW, rw, cr.Reason))
			_ = s.Put(sh)
			continue
		}

		return sh, nil
	}

	return nil, fmt.Errorf("shard %s: failed to find alive RW=%t host: %s", key.Name, rw, strings.Join(totalMsg, ";"))
}

func (s *DBPool) selectReadOnlyShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.Shard, error) {
	return s.selectShardHost(clid, key, hosts, false)
}

func (s *DBPool) selectReadWriteShardHost(clid uint, key kr.ShardKey, hosts []config.Host, tsa tsa.TSA) (shard.Shard, error) {
	return s.selectShardHost(clid, key, hosts, true)
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
		totalMsg := make([]string, 0)
		for _, host := range hostOrder {
			shard, err := s.pool.ConnectionHost(clid, key, host)
			if err != nil {
				totalMsg = append(totalMsg, fmt.Sprintf("host %s: %s", host, err.Error()))
				continue
			}
			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within: %s", strings.Join(totalMsg, ", "))
	case config.TargetSessionAttrsRO:
		return s.selectReadOnlyShardHost(clid, key, hostOrder, effectiveTargetSessionAttrs)
	case config.TargetSessionAttrsPS:
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

func (s *DBPool) BuildHostOrder(key kr.ShardKey, rw bool) ([]config.Host, error) {
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

		result := s.checker.GetCachedResults()

		// Check if we have cached results for this TSA key
		if cachedResult, exists := result[tsaKey]; exists {
			// Sort by preference: Alive first, then RW matching requirement
			if cachedResult.Alive && cachedResult.RW == rw {
				posCache = append(posCache, host)
			} else {
				negCache = append(negCache, host)
			}
		} else {
			// No cached result, add to negative cache
			negCache = append(negCache, host)
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
		pool:         NewPool(allocator),
		shardMapping: mapping,
		ShuffleHosts: true,
		PreferAZ:     preferAZ,
		checker:      tsa.NewTSAChecker(),
	}
}

// TODO: add shuffle host support here
func NewDBPoolFromMultiPool(mapping map[string]*config.Shard, sp *startup.StartupParams, mp MultiShardPool, tsaRecheckDuration time.Duration) *DBPool {
	return &DBPool{
		pool:         mp,
		shardMapping: mapping,
		checker:      tsa.NewTSACheckerWithDuration(tsaRecheckDuration),
	}
}

// TODO: add shuffle host support here
func NewDBPoolWithAllocator(mapping map[string]*config.Shard, startupParams *startup.StartupParams, allocator ConnectionAllocFn) *DBPool {
	return &DBPool{
		pool:         NewPool(allocator),
		shardMapping: mapping,
		checker:      tsa.NewTSAChecker(),
	}
}
