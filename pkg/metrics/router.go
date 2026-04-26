package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	configReloadsTotalName  = MetricPrefix + "reloads_total"
	inboundQueriesTotalName = MetricPrefix + "inbound_queries_total"
)

type RouterMetricRegistry struct {
	registry *prometheus.Registry

	configReloads     prometheus.Counter
	inboundQueries    prometheus.Counter
	registeredDynamic map[string]struct{}
}

func NewRouterMetricRegistry(registry *prometheus.Registry) *RouterMetricRegistry {

	configReloads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: configReloadsTotalName,
		Help: "Config reloads",
	},
	)
	inboundQueries := prometheus.NewCounter(prometheus.CounterOpts{
		Name: inboundQueriesTotalName,
		Help: "Number of incoming queries",
	},
	)

	registry.MustRegister(configReloads)
	registry.MustRegister(inboundQueries)

	return &RouterMetricRegistry{
		registry:          registry,
		configReloads:     configReloads,
		inboundQueries:    inboundQueries,
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

func (m *RouterMetricRegistry) IncInboundQueries() {
	m.inboundQueries.Inc()
}
