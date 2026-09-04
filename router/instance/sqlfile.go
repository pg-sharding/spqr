package instance

import (
	"bufio"
	"context"
	"os"
	"strings"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/session"
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

	r.Initialize()

	return nil
}

func NewInitSQLMetadataBootstrapper(initSQLFIle string, exitOnInitSQLError bool) *InitSQLMetadataBootstrapper {
	return &InitSQLMetadataBootstrapper{
		InitSQLFIle:        initSQLFIle,
		exitOnInitSQLError: exitOnInitSQLError,
	}
}

var _ RouterMetadataBootstrapper = &InitSQLMetadataBootstrapper{}

type AutoConfBootstrapper struct {
	AutoConfFile string
}

/* Guc processing is slightly different that console SQL, so use separate util */
func (a *AutoConfBootstrapper) InitializeMetadata(_ context.Context, _ RouterInstance) error {
	if len(a.AutoConfFile) == 0 {
		return nil
	}
	f, err := os.Open(a.AutoConfFile)
	if err != nil {
		/* XXX: Touch it? */
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(f)

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		val := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		/* unsuccessfully applied GUC can be a sign of ill-formed autoconf
		* file, so reject anything after first failure. */
		if err := session.ApplyAutoConfGUC(name, val); err != nil {
			spqrlog.Zero.Error().Err(err).Str("filename", a.AutoConfFile).Str("name", name).Str("value", val).Msg("autoconf apply failed")
			return err
		}
		count++
	}
	spqrlog.Zero.Info().Str("filename", a.AutoConfFile).Int("count", count).Msg("executed autoconf")
	return scanner.Err()
}

func NewAutoConfBootstrapper(autoConfFile string) *AutoConfBootstrapper {
	return &AutoConfBootstrapper{AutoConfFile: autoConfFile}
}

var _ RouterMetadataBootstrapper = &AutoConfBootstrapper{}
