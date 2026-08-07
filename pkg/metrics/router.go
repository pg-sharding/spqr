package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ConfigReloadsTotalName        = MetricPrefix + "reloads_total"
	InboundQueriesTotalName       = MetricPrefix + "inbound_queries_total"
	ClientConnectionsTCPTotalName = MetricPrefix + "router_client_conn_tcp_total"
	ActiveTCPCountName            = MetricPrefix + "spqr_router_client_conn_active_tcp"
	RouterTimeSummaryName         = MetricPrefix + "router_time"
	ShardTimeSummaryName          = MetricPrefix + "shard_time"
	CancelRequestCountName        = MetricPrefix + "canceled_requests_total"
	ClientInitFailCountName       = MetricPrefix + "client_init_fail_count_total"
	ClientAuthFailCountName       = MetricPrefix + "client_auth_fail_count_total"
)

type RouterMetricRegistry struct {
	registry *prometheus.Registry

	configReloads     prometheus.Counter
	registeredDynamic map[string]struct{}
}

func NewRouterMetricRegistry(registry *prometheus.Registry) *RouterMetricRegistry {

	configReloads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: ConfigReloadsTotalName,
		Help: "Config reloads",
	},
	)

	registry.MustRegister(configReloads)

	return &RouterMetricRegistry{
		registry:          registry,
		configReloads:     configReloads,
		registeredDynamic: make(map[string]struct{}),
	}
}

func (m *RouterMetricRegistry) GetRegistry() *prometheus.Registry {
	return m.registry
}

func (m *RouterMetricRegistry) RegisterDynamicGauge(gauge *DynamicGauge) {
	if _, ok := m.registeredDynamic[gauge.Name]; !ok {
		m.registeredDynamic[gauge.Name] = struct{}{}
		m.registry.MustRegister(gauge)
	}
}

func (m *RouterMetricRegistry) RegisterDynamicCounter(counter *DynamicCounter) {
	if _, ok := m.registeredDynamic[counter.Name]; !ok {
		m.registeredDynamic[counter.Name] = struct{}{}
		m.registry.MustRegister(counter)
	}
}

func (m *RouterMetricRegistry) RegisterDynamicSummary(summary *DynamicSummary) {
	if _, ok := m.registeredDynamic[summary.Name]; !ok {
		m.registeredDynamic[summary.Name] = struct{}{}
		m.registry.MustRegister(summary)
	}
}

func (m *RouterMetricRegistry) IncConfigReloads() {
	m.configReloads.Inc()
}
