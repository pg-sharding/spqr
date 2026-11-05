package router_util

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

func ReloadRotateLog() {
	spqrlog.ReloadRouterLoggers(
		config.RouterConfig().LogFileName,
		config.RouterConfig().WatchdogLogFile,
		config.RouterConfig().LogLevel,
		true, /* XXX: Never sync log in router */
		config.RouterConfig().PrettyLogging,
	)
	spqrlog.ReloadSLogger(config.RouterConfig().LogMinDurationStatement)
}
