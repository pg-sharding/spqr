package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ConfigReloadsTotalName        = MetricPrefix + "reloads_total"
	InboundQueriesTotalName       = MetricPrefix + "inbound_queries_total"
	ClientConnectionsTcpTotalName = MetricPrefix + "router_client_conn_tcp_total"
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

func (m *RouterMetricRegistry) IncConfigReloads() {
	m.configReloads.Inc()
}
