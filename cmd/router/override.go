package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/spf13/cobra"
)

type Overrides struct {
	LogLevel                     *string
	MemqdbBackupPath             *string
	RouterPort                   *int
	RouterROPort                 *int
	AdminPort                    *int
	GrpcPort                     *int
	DefaultRouteBehaviour        *string
	ShowNoticeMessages           *bool
	PgprotoDebug                 *bool
	PrettyLogging                *bool
	WithCoordinator              *bool
	EnhancedMultiShardProcessing *bool
}

func collectOverrides(cmd *cobra.Command) Overrides {
	ov := Overrides{}

	if cmd.Flags().Changed("log-level") {
		ov.LogLevel = &logLevel
	}
	if cmd.Flags().Changed("memqdb-backup-path") {
		ov.MemqdbBackupPath = &memqdbBackupPath
	}
	if cmd.Flags().Changed("router-port") {
		ov.RouterPort = &routerPort
	}
	if cmd.Flags().Changed("router-ro-port") {
		ov.RouterROPort = &routerROPort
	}
	if cmd.Flags().Changed("admin-port") {
		ov.AdminPort = &adminPort
	}
	if cmd.Flags().Changed("grpc-port") {
		ov.GrpcPort = &grpcPort
	}
	if cmd.Flags().Changed("default-route-behaviour") {
		ov.DefaultRouteBehaviour = &defaultRouteBehaviour
	}
	if cmd.Flags().Changed("show-notice-messages") {
		ov.ShowNoticeMessages = &showNoticeMessages
	}
	if cmd.Flags().Changed("pgproto-debug") {
		ov.PgprotoDebug = &pgprotoDebug
	}
	if cmd.Flags().Changed("pretty-log") {
		ov.PrettyLogging = &prettyLogging
	}
	if cmd.Flags().Changed("with-coordinator") {
		ov.WithCoordinator = &withCoord
	}
	if cmd.Flags().Changed("enhanced_multishard_processing") {
		ov.EnhancedMultiShardProcessing = &enhancedMultishardProcessing
	}

	return ov
}

func ApplyOverrides(cfg *config.Router, ov Overrides, qdbImpl string) error {
	if ov.LogLevel != nil && *ov.LogLevel != "" {
		cfg.LogLevel = *ov.LogLevel
	}
	if ov.PrettyLogging != nil {
		cfg.PrettyLogging = *ov.PrettyLogging
	}
	if ov.WithCoordinator != nil {
		cfg.WithCoordinator = *ov.WithCoordinator
	}

	if ov.MemqdbBackupPath != nil && *ov.MemqdbBackupPath != "" {
		if qdbImpl == "etcd" {
			return fmt.Errorf("cannot use memqdb-backup-path with etcdqdb")
		}
		cfg.MemqdbBackupPath = *ov.MemqdbBackupPath
	}

	if ov.PgprotoDebug != nil {
		cfg.PgprotoDebug = cfg.PgprotoDebug || *ov.PgprotoDebug
	}
	if ov.ShowNoticeMessages != nil {
		cfg.ShowNoticeMessages = cfg.ShowNoticeMessages || *ov.ShowNoticeMessages
	}

	if ov.RouterPort != nil && *ov.RouterPort != 0 {
		cfg.RouterPort = strconv.FormatInt(int64(*ov.RouterPort), 10)
	}
	if ov.RouterROPort != nil && *ov.RouterROPort != 0 {
		cfg.RouterROPort = strconv.FormatInt(int64(*ov.RouterROPort), 10)
	}
	if ov.AdminPort != nil && *ov.AdminPort != 0 {
		cfg.AdminConsolePort = strconv.FormatInt(int64(*ov.AdminPort), 10)
	}
	if ov.GrpcPort != nil && *ov.GrpcPort != 0 {
		cfg.GrpcApiPort = strconv.FormatInt(int64(*ov.GrpcPort), 10)
	}

	if ov.DefaultRouteBehaviour != nil && *ov.DefaultRouteBehaviour != "" {
		if strings.ToLower(*ov.DefaultRouteBehaviour) == "block" {
			cfg.Qr.DefaultRouteBehaviour = config.DefaultRouteBehaviourBlock
		} else {
			cfg.Qr.DefaultRouteBehaviour = config.DefaultRouteBehaviourAllow
		}
	}

	if ov.EnhancedMultiShardProcessing != nil {
		cfg.Qr.EnhancedMultiShardProcessing = *ov.EnhancedMultiShardProcessing
	}

	return nil
}

func logEffectiveConfig(cfg *config.Router) error {
	configBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	log.Println("Running config:", string(configBytes))
	return nil
}
