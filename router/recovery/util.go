package recovery

import "github.com/pg-sharding/spqr/pkg/config"

func GetWatchdogBackendRules() []*config.BackendRule {
	if config.RouterConfig().WatchdogBackendRule != nil {
		return []*config.BackendRule{config.RouterConfig().WatchdogBackendRule}
	} else {
		return config.RouterConfig().BackendRules
	}
}
