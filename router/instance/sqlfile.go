package instance

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/client"
)

type InitSQLMetadataBootstraper struct {
	InitSQLFIle string
}

// InitializeMetadata implements RouterMetadataBootstraper.
func (i *InitSQLMetadataBootstraper) InitializeMetadata(ctx context.Context, r RouterInstance) error {
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
			if err := r.Console().ProcessQuery(ctx, query, client.NewFakeClient()); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
		}

		spqrlog.Zero.Info().
			Int("count", len(queries)).
			Str("filename", fname).
			Msg("successfully init queries from file")
	}

	r.Initialize()

	return nil
}

func NewInitSQLMetadataBootstraper(InitSQLFIle string) RouterMetadataBootstraper {
	return &InitSQLMetadataBootstraper{
		InitSQLFIle: InitSQLFIle,
	}
}

var _ RouterMetadataBootstraper = &InitSQLMetadataBootstraper{}
