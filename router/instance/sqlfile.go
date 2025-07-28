package instance

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/client"
)

type InitSQLMetadataBootstrapper struct {
	InitSQLFIle        string
	exitOnInitSQLError bool
}

// InitializeMetadata implements RouterMetadataBootstrapper.
func (i *InitSQLMetadataBootstrapper) InitializeMetadata(ctx context.Context, r RouterInstance) error {
	for _, fname := range []string{
		// rcfg.InitSQL,
		i.InitSQLFIle,
	} {
		if len(fname) == 0 {
			continue
		}
		queries, err := r.Console().Qlog().Recover(ctx, fname)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize router")
			return err
		}

		spqrlog.Zero.Info().Msg("executing init sql")
		for _, query := range queries {
			spqrlog.Zero.Info().Str("query", query).Msg("")
			if err := r.Console().ProcessQuery(ctx, query, client.NewFakeClient(), &catalog.FakeChecker{}); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				if i.exitOnInitSQLError {
					return err
				}
			}
		}

		spqrlog.Zero.Info().
			Int("count", len(queries)).
			Str("filename", fname).
			Msg("successfully init queries from file")
	}

	r.Open()

	return nil
}

func NewInitSQLMetadataBootstrapper(initSQLFIle string, exitOnInitSQLError bool) *InitSQLMetadataBootstrapper {
	return &InitSQLMetadataBootstrapper{
		InitSQLFIle:        initSQLFIle,
		exitOnInitSQLError: exitOnInitSQLError,
	}
}

var _ RouterMetadataBootstrapper = &InitSQLMetadataBootstrapper{}
