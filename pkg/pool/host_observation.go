package pool

import (
	"reflect"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type hostIdentity struct{ shard, host, az string }

type hostAttempt struct {
	hostIdentity
	generation      uint64
	shardGeneration uint64
	sequence        uint64
	definition      *topology.DataShard
	options         []topology.GenericOption
	started         time.Time
	required        tsa.TSA
	matchOverride   bool
	requiredMatch   bool
	requiredSet     bool
}

func (s *DBPool) routeIdentity() (string, string) {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	if s.backendRule == nil {
		return "", ""
	}
	return s.backendRule.Usr, s.backendRule.DB
}

func (s *DBPool) probeConfig() (*config.BackendRule, *startup.StartupParams) {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	return s.backendRule, s.startupParams
}

func (s *DBPool) beginObservation(shard string, host config.Host) hostAttempt {
	return s.newObservation(shard, host, true)
}
func (s *DBPool) newObservation(shard string, host config.Host, active bool) hostAttempt {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	if s.attempts == nil {
		s.attempts = make(map[hostIdentity]uint64)
	}
	if s.observations == nil {
		s.observations = make(map[hostIdentity]tsa.CachedCheckResult)
	}
	id := hostIdentity{shard, host.Address, host.AZ}
	s.attemptSequence++
	if active {
		s.attempts[id] = s.attemptSequence
	}
	token := hostAttempt{hostIdentity: id, generation: s.generation, shardGeneration: s.shardGenerations[shard], sequence: s.attemptSequence, started: time.Now(), required: config.TargetSessionAttrsRW, requiredSet: true}
	if sh, err := s.tmgr.ShardById(shard); err == nil {
		token.definition = sh
		token.options = append([]topology.GenericOption(nil), sh.Options()...)
	}
	return token
}

// publishObservation accepts one attempt into both health and TSA snapshots.
// The attempt began before network I/O, so a late result cannot overwrite a
// newer attempt, even when the latter failed or its target was removed.
func (s *DBPool) publishObservation(token hostAttempt, cr tsa.CheckResult, stage string, checked time.Time) bool {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	if s.observationsClosed || token.generation != s.generation || token.shardGeneration != s.shardGenerations[token.shard] || s.attempts[token.hostIdentity] > token.sequence {
		return false
	}
	sh, err := s.tmgr.ShardById(token.shard)
	if err != nil || token.definition != sh || !reflect.DeepEqual(token.options, sh.Options()) {
		return false
	}
	found := false
	for _, host := range sh.HostsAZ() {
		if host.Address == token.host && host.AZ == token.az {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	s.attempts[token.hostIdentity] = token.sequence
	previous, exists := s.observations[token.hostIdentity]
	if exists && checked.Before(previous.LastCheckTime) {
		return false
	}
	observation := tsa.CachedCheckResult{CR: cr, LastCheckTime: checked, LastSuccessTime: previous.LastSuccessTime, FailureStage: stage}
	if cr.Alive {
		observation.LastSuccessTime = checked
	}
	s.observations[token.hostIdentity] = observation
	s.checker.PublishScoped(token.shard, token.host, token.az, observation)
	var required []tsa.TSA
	if token.requiredSet {
		required = append(required, token.required)
	}
	s.cache.publish(token.shard, token.host, token.az, checked, func(required tsa.TSA) LocalCheckResult {
		match := s.evaluateTSAMatch(cr, required)
		if token.matchOverride && required == token.required {
			match = cr.Alive && token.requiredMatch
		}
		return LocalCheckResult{Alive: cr.Alive, Match: match, Reason: cr.Reason}
	}, required...)
	if !exists || previous.CR.Alive != cr.Alive || previous.CR.RW != cr.RW || previous.FailureStage != stage || previous.CR.Reason != cr.Reason {
		user, db := "", ""
		if s.backendRule != nil {
			user = s.backendRule.Usr
			db = s.backendRule.DB
		}
		spqrlog.Zero.Info().Str("user", user).Str("database", db).Str("shard", token.shard).Str("host", token.host).Str("az", token.az).Bool("alive", cr.Alive).Bool("rw", cr.RW).Str("stage", stage).Str("reason", cr.Reason).Msg("backend host observation changed")
	}
	return true
}

func (s *DBPool) clearShardObservations(shard string) {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	if s.shardGenerations == nil {
		s.shardGenerations = map[string]uint64{}
	}
	s.shardGenerations[shard]++
	for id := range s.observations {
		if id.shard == shard {
			delete(s.observations, id)
			delete(s.attempts, id)
			s.checker.RemoveScoped(id.shard, id.host, id.az)
		}
	}
	// Remove attempts that have not produced a result yet as well.
	for id := range s.attempts {
		if id.shard == shard {
			delete(s.attempts, id)
		}
	}
	s.cache.removeShard(shard)
}

func (s *DBPool) requestPrimaryCheck(shard string) {
	s.observationMu.Lock()
	monitor := s.monitor
	s.observationMu.Unlock()
	if monitor != nil {
		monitor.Request(s, shard)
	}
}

func (c *DbpoolCache) publish(shard, host, az string, at time.Time, evaluate func(tsa.TSA) LocalCheckResult, extra ...tsa.TSA) {
	required := map[tsa.TSA]bool{}
	for _, value := range extra {
		required[value] = true
	}
	c.cache.Range(func(key, _ any) bool {
		k := key.(TsaKey)
		if k.Host == host && k.AZ == az && (k.Shard == shard || k.Shard == "") {
			required[k.Tsa] = true
			if k.Shard == "" {
				c.cache.Delete(key)
			}
		}
		return true
	})
	for requiredTSA := range required {
		c.cache.Store(TsaKey{Shard: shard, Host: host, AZ: az, Tsa: requiredTSA}, CachedEntry{LastCheckTime: at, Result: evaluate(requiredTSA)})
	}
}

func (c *DbpoolCache) removeShard(shard string) {
	c.cache.Range(func(key, _ any) bool {
		if key.(TsaKey).Shard == shard {
			c.cache.Delete(key)
		}
		return true
	})
}

func (c *DbpoolCache) matchShard(shard string, required tsa.TSA, host, az string) (LocalCheckResult, bool) {
	if raw, ok := c.cache.Load(TsaKey{Shard: shard, Tsa: required, Host: host, AZ: az}); ok {
		entry := raw.(CachedEntry)
		if time.Since(entry.LastCheckTime) <= c.cacheTTL {
			return entry.Result, true
		}
	}
	if raw, ok := c.cache.Load(TsaKey{Tsa: required, Host: host, AZ: az}); ok {
		entry := raw.(CachedEntry)
		if time.Since(entry.LastCheckTime) <= c.cacheTTL {
			return entry.Result, true
		}
	}
	return LocalCheckResult{}, false
}

// A manual recheck retains its failed-entry scope and SQL rows, but performs
// only one probe for all TSA variants of a given shard/host/availability zone.
func (s *DBPool) recheckMonitoredHosts(monitor *PrimaryMonitor, entries map[TsaKey]CachedEntry, interval time.Duration) []HostRecheckResult {
	grouped := map[hostIdentity][]TsaKey{}
	snapshot := s.tmgr.Snap()
	for key, entry := range entries {
		if entry.Result.Alive || time.Since(entry.LastCheckTime) <= interval {
			continue
		}
		for name, sh := range snapshot {
			if key.Shard != "" && key.Shard != name {
				continue
			}
			for _, host := range sh.HostsAZ() {
				if host.Address == key.Host && host.AZ == key.AZ {
					id := hostIdentity{name, host.Address, host.AZ}
					grouped[id] = append(grouped[id], key)
					break
				}
			}
		}
	}
	var results []HostRecheckResult
	for id, keys := range grouped {
		raw, ok := monitor.recheckHost(s, id.shard, config.Host{Address: id.host, AZ: id.az})
		if !ok {
			continue
		}
		cr := tsa.CheckResult{Alive: raw.Alive, RW: raw.Match, Reason: raw.Reason}
		for _, key := range keys {
			result := raw
			result.Tsa = key.Tsa
			result.Match = s.evaluateTSAMatch(cr, key.Tsa)
			results = append(results, result)
		}
	}
	return results
}

// Cache hits can seed a newly requested TSA variant, but must never advance the
// attempt order or replace a newer network observation.
func (s *DBPool) seedTSA(shard string, host config.Host, required tsa.TSA, cached tsa.CachedCheckResult, matched bool) {
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	current, ok := s.observations[hostIdentity{shard, host.Address, host.AZ}]
	if !ok || s.observationsClosed || !current.LastCheckTime.Equal(cached.LastCheckTime) {
		return
	}
	s.cache.publish(shard, host.Address, host.AZ, current.LastCheckTime, func(value tsa.TSA) LocalCheckResult {
		match := s.evaluateTSAMatch(current.CR, value)
		if value == required {
			match = current.CR.Alive && matched
		}
		return LocalCheckResult{Alive: current.CR.Alive, Match: match, Reason: current.CR.Reason}
	}, required)
}
