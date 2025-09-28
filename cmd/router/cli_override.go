package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/spf13/cobra"
)

type overrideRule struct {
	name     string
	changed  func() bool
	validate func() error
	apply    func()
}

func boolOR(dst *bool, add bool) { *dst = *dst || add }
func setIfNotZero(dst *string, v int) {
	if v != 0 {
		*dst = strconv.FormatInt(int64(v), 10)
	}
}

func buildOverrideRules(cmd *cobra.Command, cfg *config.Router) []overrideRule {
	return []overrideRule{
		{
			name:    "log-level",
			changed: func() bool { return cmd.Flags().Changed("log-level") },
			apply:   func() { cfg.LogLevel = logLevel },
		},
		{
			name:    "pretty-log",
			changed: func() bool { return cmd.Flags().Changed("pretty-log") },
			apply:   func() { cfg.PrettyLogging = prettyLogging },
		},
		{
			name:    "with-coordinator",
			changed: func() bool { return cmd.Flags().Changed("with-coordinator") },
			apply:   func() { cfg.WithCoordinator = withCoord },
		},
		{
			name:    "memqdb-backup-path",
			changed: func() bool { return cmd.Flags().Changed("memqdb-backup-path") },
			validate: func() error {
				if memqdbBackupPath != "" && qdbImpl == "etcd" {
					return fmt.Errorf("cannot use memqdb-backup-path with etcdqdb")
				}
				return nil
			},
			apply: func() {
				if memqdbBackupPath != "" {
					cfg.MemqdbBackupPath = memqdbBackupPath
				}
			},
		},
		{
			name:    "pgproto-debug",
			changed: func() bool { return cmd.Flags().Changed("pgproto-debug") },
			apply:   func() { boolOR(&cfg.PgprotoDebug, pgprotoDebug) },
		},
		{
			name:    "show-notice-messages",
			changed: func() bool { return cmd.Flags().Changed("show-notice-messages") },
			apply:   func() { boolOR(&cfg.ShowNoticeMessages, showNoticeMessages) },
		},
		{
			name:    "router-port",
			changed: func() bool { return cmd.Flags().Changed("router-port") },
			apply:   func() { setIfNotZero(&cfg.RouterPort, routerPort) },
		},
		{
			name:    "router-ro-port",
			changed: func() bool { return cmd.Flags().Changed("router-ro-port") },
			apply:   func() { setIfNotZero(&cfg.RouterROPort, routerROPort) },
		},
		{
			name:    "admin-port",
			changed: func() bool { return cmd.Flags().Changed("admin-port") },
			apply:   func() { setIfNotZero(&cfg.AdminConsolePort, adminPort) },
		},
		{
			name:    "grpc-port",
			changed: func() bool { return cmd.Flags().Changed("grpc-port") },
			apply:   func() { setIfNotZero(&cfg.GrpcApiPort, grpcPort) },
		},
		{
			name:    "default-route-behaviour",
			changed: func() bool { return cmd.Flags().Changed("default-route-behaviour") },
			apply: func() {
				if defaultRouteBehaviour != "" {
					if strings.ToLower(defaultRouteBehaviour) == "block" {
						cfg.Qr.DefaultRouteBehaviour = config.DefaultRouteBehaviourBlock
					} else {
						cfg.Qr.DefaultRouteBehaviour = config.DefaultRouteBehaviourAllow
					}
				}
			},
		},
		{
			name:    "enhanced_multishard_processing",
			changed: func() bool { return cmd.Flags().Changed("enhanced_multishard_processing") },
			apply:   func() { cfg.Qr.EnhancedMultiShardProcessing = enhancedMultishardProcessing },
		},
	}
}

func applyOverrides(cmd *cobra.Command, cfg *config.Router) error {
	rules := buildOverrideRules(cmd, cfg)
	for _, r := range rules {
		if r.changed() && r.validate != nil {
			if err := r.validate(); err != nil {
				return fmt.Errorf("%s: %w", r.name, err)
			}
		}
	}
	for _, r := range rules {
		if r.changed() {
			r.apply()
		}
	}
	return nil
}
