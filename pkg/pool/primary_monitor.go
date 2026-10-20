package pool

import (
	"context"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/prometheus/client_golang/prometheus"
)

type primaryProbeFn func(context.Context, topology.TopologyMgr, *startup.StartupParams, *config.BackendRule, kr.ShardKey, config.Host) (tsa.CheckResult, string, error)

type primaryTarget struct {
	pool        *DBPool
	shard       string
	options     []topology.GenericOption
	definition  *topology.DataShard
	ctx         context.Context
	cancel      context.CancelFunc
	due         time.Time
	running     bool
	requested   bool
	hosts       []config.Host
	next        int
	preferred   config.Host
	passStarted time.Time
	manual      []primaryManual
}

type primaryManual struct {
	queued time.Time
	host   config.Host
	result chan HostRecheckResult
}

type primaryJob struct {
	target *primaryTarget
	host   config.Host
	queued time.Time
	manual *primaryManual
}

// PrimaryMonitor owns the router's probe workers. Each job checks one candidate,
// allowing other shards to run between candidates in a discovery pass.
type PrimaryMonitor struct {
	mu       sync.Mutex
	targets  map[*DBPool]map[string]*primaryTarget
	interval time.Duration
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wake     chan struct{}
	jobs     chan primaryJob
	wg       sync.WaitGroup
	probe    primaryProbeFn
	counts   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	delay    *prometheus.HistogramVec
	success  *prometheus.GaugeVec
}

func NewPrimaryMonitor(interval, timeout time.Duration, concurrency int) *PrimaryMonitor {
	if timeout <= 0 {
		timeout = config.DefaultDbpoolPrimaryCheckTimeout
	}
	if concurrency <= 0 {
		concurrency = config.DefaultDbpoolPrimaryCheckConcurrency
	}
	ctx, cancel := context.WithCancel(context.Background())
	labels := []string{"user", "database", "shard"}
	m := &PrimaryMonitor{targets: make(map[*DBPool]map[string]*primaryTarget), interval: interval, timeout: timeout, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), jobs: make(chan primaryJob), probe: probePrimary,
		counts:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "spqr_primary_probe_total", Help: "Primary probes by outcome."}, append(append([]string{}, labels...), "outcome")),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "spqr_primary_probe_duration_seconds", Help: "Primary probe duration, including connection establishment."}, labels),
		delay:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "spqr_primary_probe_scheduling_delay_seconds", Help: "Time from a probe becoming due until a worker starts it."}, labels),
		success:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "spqr_primary_last_success_timestamp_seconds", Help: "Timestamp of the last successful writable check; zero until discovery."}, labels),
	}
	m.wg.Add(concurrency + 1)
	for range concurrency {
		go m.worker()
	}
	go m.schedule()
	return m
}

func (m *PrimaryMonitor) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Register is called only after the route wins registration and SetRule completes.
func (m *PrimaryMonitor) Register(p *DBPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return
	}
	if _, ok := m.targets[p]; ok {
		return
	}
	p.observationMu.Lock()
	if p.monitorDisabled || p.backendRule == nil || p.observationsClosed {
		p.observationMu.Unlock()
		return
	}
	p.monitor = m
	p.observationMu.Unlock()
	m.targets[p] = make(map[string]*primaryTarget)
	m.reconcileLocked(p, time.Now())
	m.signal()
}

func (m *PrimaryMonitor) removeTargetLocked(t *primaryTarget) {
	t.cancel()
	t.pool.clearShardObservations(t.shard)
	user, db := t.pool.routeIdentity()
	m.counts.DeletePartialMatch(prometheus.Labels{"user": user, "database": db, "shard": t.shard})
	m.duration.DeleteLabelValues(user, db, t.shard)
	m.delay.DeleteLabelValues(user, db, t.shard)
	m.success.DeleteLabelValues(user, db, t.shard)
}

func (m *PrimaryMonitor) Unregister(p *DBPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.targets[p] {
		m.removeTargetLocked(t)
	}
	delete(m.targets, p)
	p.observationMu.Lock()
	p.generation++
	p.monitor = nil
	p.observationsClosed = true
	p.observationMu.Unlock()
	m.signal()
}

func (m *PrimaryMonitor) reconcileLocked(p *DBPool, now time.Time) {
	targets := m.targets[p]
	snapshot := p.tmgr.Snap()
	for name, t := range targets {
		sh, ok := snapshot[name]
		if !ok || t.definition != sh || !reflect.DeepEqual(t.options, sh.Options()) {
			m.removeTargetLocked(t)
			delete(targets, name)
		}
	}
	for name, sh := range snapshot {
		if _, ok := targets[name]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(m.ctx)
		t := &primaryTarget{pool: p, shard: name, definition: sh, options: append([]topology.GenericOption(nil), sh.Options()...), ctx: ctx, cancel: cancel, due: now}
		targets[name] = t
		user, db := p.routeIdentity()
		m.success.WithLabelValues(user, db, name).Set(0)
	}
}

// Request coalesces early checks without blocking the requesting application.
func (m *PrimaryMonitor) Request(p *DBPool, shard string) {
	if m.interval <= 0 {
		return
	}
	m.mu.Lock()
	if t := m.targets[p][shard]; t != nil && t.ctx.Err() == nil {
		if t.running || t.hosts != nil {
			t.requested = true
		} else {
			t.due = time.Now()
		}
	}
	m.mu.Unlock()
	m.signal()
}

func (m *PrimaryMonitor) recheckHost(p *DBPool, shard string, host config.Host) (HostRecheckResult, bool) {
	response := make(chan HostRecheckResult, 1)
	m.mu.Lock()
	targets, ok := m.targets[p]
	if !ok {
		m.mu.Unlock()
		return HostRecheckResult{}, false
	}
	m.reconcileLocked(p, time.Now())
	t := targets[shard]
	if t == nil {
		m.mu.Unlock()
		return HostRecheckResult{}, false
	}
	t.manual = append(t.manual, primaryManual{host: host, result: response, queued: time.Now()})
	m.mu.Unlock()
	m.signal()
	select {
	case result := <-response:
		return result, true
	case <-t.ctx.Done():
		return HostRecheckResult{}, false
	}
}

func (m *PrimaryMonitor) schedule() {
	defer m.wg.Done()
	// Reconciliation is independent of probe progress, including when disabled and
	// only manual rechecks are active. Publication also validates the live topology.
	tick := 100 * time.Millisecond
	if m.interval > 0 && m.interval/10 < tick {
		tick = m.interval / 10
	}
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	var reconcileAt time.Time
	for {
		now := time.Now()
		m.mu.Lock()
		var value primaryJob
		haveJob := false
		if !now.Before(reconcileAt) {
			for p := range m.targets {
				m.reconcileLocked(p, now)
			}
			reconcileAt = now.Add(tick)
		}
		for p, targets := range m.targets {
			for _, t := range targets {
				if t.running {
					continue
				}
				if len(t.manual) > 0 {
					manual := t.manual[0]
					t.manual = t.manual[1:]
					value = primaryJob{target: t, host: manual.host, queued: manual.queued, manual: &manual}
					haveJob = true
					break
				}
				if m.interval <= 0 || now.Before(t.due) {
					continue
				}
				if t.hosts == nil {
					hosts, err := p.BuildHostOrder(kr.ShardKey{Name: t.shard}, config.TargetSessionAttrsRW)
					if err != nil {
						t.due = now.Add(m.interval)
						continue
					}
					t.hosts = hosts
					if t.preferred.Address != "" {
						for i, h := range hosts {
							if h.Address == t.preferred.Address && h.AZ == t.preferred.AZ {
								t.hosts = append([]config.Host{h}, append(hosts[:i:i], hosts[i+1:]...)...)
								break
							}
						}
					}
					t.next = 0
					t.passStarted = now
				}
				if t.next >= len(t.hosts) {
					m.finishPassLocked(t, now)
					continue
				}
				candidate := primaryJob{target: t, host: t.hosts[t.next], queued: t.due}
				if !haveJob || candidate.queued.Before(value.queued) {
					value = candidate
					haveJob = true
				}
			}
			if haveJob && value.manual != nil {
				break
			}
		}
		m.mu.Unlock()
		var jobs chan primaryJob
		if haveJob {
			jobs = m.jobs
		}
		// Mark busy before making the job visible. No worker waits on an application
		// connection or on a second scheduler budget.
		if haveJob {
			m.mu.Lock()
			if value.target.ctx.Err() == nil {
				value.target.running = true
			} else {
				jobs = nil
			}
			m.mu.Unlock()
		}
		select {
		case <-m.ctx.Done():
			return
		case jobs <- value:
			// Dispatch another independent shard immediately when a worker is free.
			m.signal()
		case <-m.wake:
			if haveJob {
				m.mu.Lock()
				value.target.running = false
				if value.manual != nil {
					value.target.manual = append([]primaryManual{*value.manual}, value.target.manual...)
				}
				m.mu.Unlock()
			}
		case <-ticker.C:
			if haveJob {
				m.mu.Lock()
				value.target.running = false
				if value.manual != nil {
					value.target.manual = append([]primaryManual{*value.manual}, value.target.manual...)
				}
				m.mu.Unlock()
			}
		}
	}
}

func (m *PrimaryMonitor) finishPassLocked(t *primaryTarget, now time.Time) {
	t.hosts = nil
	t.next = 0
	if t.requested {
		t.due = now
		t.requested = false
		return
	}
	// A small symmetric jitter spreads connections across routes and shards.
	jitter := time.Duration(float64(m.interval) * (0.9 + rand.Float64()*0.2))
	t.due = t.passStarted.Add(jitter)
}

func (m *PrimaryMonitor) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case job := <-m.jobs:
			t := job.target
			started := time.Now()
			ctx, cancel := context.WithTimeout(t.ctx, m.timeout)
			token := t.pool.beginObservation(t.shard, job.host)
			token.options = t.options
			token.definition = t.definition
			if job.manual != nil && m.interval <= 0 {
				token.requiredSet = false
			}
			rule, sp := t.pool.probeConfig()
			cr, stage, err := m.probe(ctx, t.pool.tmgr, sp, rule, kr.ShardKey{Name: t.shard}, job.host)
			cancel()
			if err != nil {
				cr.Alive = false
				cr.RW = false
				cr.Reason = err.Error()
			}
			accepted := false
			if t.ctx.Err() == nil {
				accepted = t.pool.publishObservation(token, cr, stage, started)
			}
			m.mu.Lock()
			if t.ctx.Err() == nil {
				user, db := t.pool.routeIdentity()
				outcome := "read_only"
				if err != nil || !cr.Alive {
					outcome = "failure"
				} else if cr.RW {
					outcome = "writable"
				}
				m.counts.WithLabelValues(user, db, t.shard, outcome).Inc()
				m.duration.WithLabelValues(user, db, t.shard).Observe(time.Since(started).Seconds())
				m.delay.WithLabelValues(user, db, t.shard).Observe(started.Sub(job.queued).Seconds())
				if accepted && cr.Alive && cr.RW {
					m.success.WithLabelValues(user, db, t.shard).Set(float64(started.UnixNano()) / 1e9)
				}
				if accepted && cr.Alive && cr.RW {
					t.preferred = job.host
				} else if t.preferred.Address == job.host.Address && t.preferred.AZ == job.host.AZ {
					t.preferred = config.Host{}
				}
				if job.manual != nil {
					job.manual.result <- HostRecheckResult{Host: job.host.Address, AZ: job.host.AZ, Alive: cr.Alive, Match: cr.Alive && cr.RW, Reason: cr.Reason}
				} else {
					t.next++
					if accepted && cr.Alive && cr.RW || t.next >= len(t.hosts) {
						m.finishPassLocked(t, time.Now())
					} else {
						t.due = time.Now()
					}
				}
			}
			t.running = false
			m.mu.Unlock()
			m.signal()
		}
	}
}

func (m *PrimaryMonitor) Close() {
	m.cancel()
	m.mu.Lock()
	for p, targets := range m.targets {
		for _, t := range targets {
			m.removeTargetLocked(t)
		}
		p.observationMu.Lock()
		p.monitor = nil
		p.observationsClosed = true
		p.generation++
		p.observationMu.Unlock()
	}
	clear(m.targets)
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *PrimaryMonitor) Describe(ch chan<- *prometheus.Desc) {
	m.counts.Describe(ch)
	m.duration.Describe(ch)
	m.delay.Describe(ch)
	m.success.Describe(ch)
}
func (m *PrimaryMonitor) Collect(ch chan<- prometheus.Metric) {
	m.counts.Collect(ch)
	m.duration.Collect(ch)
	m.delay.Collect(ch)
	m.success.Collect(ch)
}
