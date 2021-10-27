package pkg

import (
	"crypto/tls"
	"io"
	"net"

	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/pkg/errors"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Router interface {
	Addr() string
	ID() string
}

type RouterImpl struct {
	Rrouter rrouter.Rrouter
	Qrouter qrouter.Qrouter

	AdmConsole console.Console

	SPIexecuter *Executer
	stchan      chan struct{}
	addr        string
	frTLS       *tls.Config
}

func (r *RouterImpl) ID() string {
	return "noid"
}

func (r *RouterImpl) Addr() string {
	return r.addr
}

var _ Router = &RouterImpl{}

func NewRouter() (*RouterImpl, error) {

	// qrouter init
	qtype := config.QrouterType(config.RouterConfig().QRouterCfg.Qtype)
	tracelog.InfoLogger.Printf("create Qrouter with type %s", qtype)

	qr, err := qrouter.NewQrouter(qtype)
	if err != nil {
		return nil, err
	}

	// frontend
	frTlsCfg := config.RouterConfig().RouterConfig.TLSCfg
	frTLS, err := config.InitTLS(frTlsCfg.SslMode, frTlsCfg.CertFile, frTlsCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "init frontend TLS")
	}

	// request router
	rr, err := rrouter.NewRouter(frTLS)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}

	// data shards, world shard and sharding rules
	for name, shard := range config.RouterConfig().RouterConfig.ShardMapping {

		switch shard.ShType {
		case config.WorldShard:

			if err := rr.AddWorldShard(qdb.ShardKey{Name: name}); err != nil {
				return nil, err
			}
			if err := qr.AddWorldShard(name, shard); err != nil {
				return nil, err
			}

		case config.DataShard:
			// data shard assumed by default
			fallthrough
		default:

			if err := shard.InitShardTLS(); err != nil {
				return nil, err
			}

			if err := rr.AddDataShard(qdb.ShardKey{Name: name}); err != nil {
				return nil, err
			}
			if err := qr.AddDataShard(name, shard); err != nil {
				return nil, err
			}
		}

	}

	stchan := make(chan struct{})
	cnsl, err := console.NewConsole(frTLS, qr, stchan)
	if err != nil {
		tracelog.ErrorLogger.PrintError(xerrors.Errorf("failed to initialize router: %w", err))
		return nil, err
	}

	executer := NewExecuter(config.RouterConfig().ExecuterCfg)
	if err := executer.SPIexec(cnsl, rrouter.NewFakeClient()); err != nil {
		return nil, err
	}

	queries, err := cnsl.Qlog.Recover(config.RouterConfig().DataFolder)
	if err != nil {
		tracelog.ErrorLogger.PrintError(xerrors.Errorf("failed to initialize router: %w", err))
		return nil, err
	}

	for _, query := range queries {
		if err := cnsl.ProcessQuery(query, rrouter.NewFakeClient()); err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}

	tracelog.InfoLogger.Printf("Successfully init %d queries", len(queries))

	return &RouterImpl{
		Rrouter:     rr,
		Qrouter:     qr,
		AdmConsole:  cnsl,
		SPIexecuter: executer,
		stchan:      stchan,
		frTLS:       frTLS,
	}, nil
}

func (r *RouterImpl) serv(netconn net.Conn) error {

	client, err := r.Rrouter.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := rrouter.MatchConnectionPooler(client)
	if err != nil {
		return err
	}

	return Frontend(r.Qrouter, client, cmngr)
}

func (r *RouterImpl) Run(listener net.Listener) error {
	closer, err := r.initJaegerTracer()
	if err != nil {
		return xerrors.Errorf("could not initialize jaeger tracer: %s", err.Error())
	}
	defer func() { _ = closer.Close() }()

	cChan := make(chan net.Conn)

	accept := func(l net.Listener, cChan chan net.Conn) {
		for {
			c, err := l.Accept()
			if err != nil {
				// handle error (and then for example indicate acceptor is down)
				cChan <- nil
				return
			}
			cChan <- c
		}
	}

	go accept(listener, cChan)

	for {
		select {
		case conn := <-cChan:

			go func() {
				if err := r.serv(conn); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		case <-r.stchan:
			_ = r.Rrouter.Shutdown()
			_ = listener.Close()
		}
	}
}

func (r *RouterImpl) initJaegerTracer() (io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: "worldmock",
		Sampler: &jaegercfg.SamplerConfig{
			Type:              "const",
			Param:             1,
			SamplingServerURL: config.RouterConfig().JaegerConfig.JaegerUrl,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: false,
		},
		Gen128Bit: true,
		Tags: []opentracing.Tag{
			{Key: "span.kind", Value: "server"},
		},
	}

	jLogger := jaegerlog.StdLogger //TODO: replace with tracelog logger
	jMetricsFactory := metrics.NullFactory

	// Initialize tracer with a logger and a metrics factory
	return cfg.InitGlobalTracer(
		"worldmock",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
}

func (r *RouterImpl) servAdm(netconn net.Conn) error {
	cl := rrouter.NewPsqlClient(netconn)

	if err := cl.Init(r.frTLS, config.SSLMODEDISABLE); err != nil {
		return err
	}

	return r.AdmConsole.Serve(cl)
}

func (r *RouterImpl) RunAdm(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return errors.Wrap(err, "RunAdm failed")
		}
		go func() {
			if err := r.servAdm(conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}
