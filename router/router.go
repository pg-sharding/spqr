package router

import (
	"crypto/tls"
	"fmt"
	"github.com/pg-sharding/spqr/router/pkg"
	"io"
	"net"

	"github.com/jackc/pgproto3"
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
)


type Router interface {

}

type RouterImpl struct {
	Rrouter rrouter.Rrouter
	Qrouter qrouter.Qrouter

	ConsoleDB console.Console

	SPIexecuter *Executer
	stchan      chan struct{}
	frTLS       *tls.Config
}

var _ Router = &RouterImpl{}

func NewRouter() (*RouterImpl, error) {

	qtype := config.QrouterType(config.Get().QRouterCfg.Qtype)
	tracelog.InfoLogger.Printf("create Qrouter with type %s", qtype)

	qr, err := qrouter.NewQrouter(qtype)
	if err != nil {
		return nil, err
	}

	frTlsCfg := config.Get().RouterConfig.TLSCfg
	frTLS, err := initTLS(frTlsCfg.SslMode, frTlsCfg.CertFile, frTlsCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "init frontend TLS")
	}

	rr, err := rrouter.NewRouter(frTLS)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}

	for name, shard := range config.Get().RouterConfig.ShardMapping {
		shardTLSConfig, err := initTLS(shard.TLSCfg.SslMode, shard.TLSCfg.CertFile, shard.TLSCfg.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "init shard TLS")
		}
		shard.TLSConfig = shardTLSConfig
		_ = rr.AddShard(qdb.ShardKey{Name: name}) // TODO error handling
		if err := qr.AddShard(name, shard); err != nil {
			return nil, err
		}
	}
	stchan := make(chan struct{})

	cnsl, err := console.NewConsole(frTLS, qr, stchan)
	if err != nil {
		return nil, errors.Wrap(err, "NewConsole")
	}

	executer := NewExecuter(config.Get().ExecuterCfg)
	_ = executer.SPIexec(cnsl, rrouter.NewFakeClient()) // TODO add error handling
	queries, err := cnsl.Qlog.Recover(config.Get().DataFolder)
	if err != nil {
		tracelog.ErrorLogger.PrintError(errors.Wrap(err, "Serve can't start"))
	}

	for _, query := range queries {
		if err := cnsl.ProcessQuery(query, rrouter.NewFakeClient()); err != nil {
			continue // TODO fix 'syntax error'
			// return errors.Wrap(err, "Serve init fail")
		}
	}

	tracelog.InfoLogger.Printf("Succesfully init %d queries", len(queries))

	return &RouterImpl{
		Rrouter:     rr,
		Qrouter:     qr,
		ConsoleDB:   cnsl,
		SPIexecuter: executer,
		stchan:      stchan,
		frTLS:       frTLS,
	}, nil
}

func initTLS(sslMode, certFile, keyFile string) (*tls.Config, error) {
	if sslMode != config.SSLMODEDISABLE {
		tracelog.InfoLogger.Printf("loading tls cert file %s, key file %s", certFile, keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load tls conf")
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}, nil
	}
	return nil, nil
}

func (sg *RouterImpl) serv(netconn net.Conn) error {

	client, err := sg.Rrouter.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := rrouter.InitClConnection(client)
	if err != nil {
		return err
	}

	return pkg.Frontend(sg.Qrouter, client, cmngr)
}

func (sg *RouterImpl) Run(listener net.Listener) error {
	closer, err := sg.initJaegerTracer()
	if err != nil {
		return fmt.Errorf("could not initialize jaeger tracer: %s", err.Error())
	}
	defer func() { _ = closer.Close() }()

	cChan := make(chan net.Conn)

	accept := func(l net.Listener) {
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

	go accept(listener)

	for {

		select {
		case conn := <-cChan:

			go func() {
				if err := sg.serv(conn); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		case <-sg.stchan:
			_ = sg.Rrouter.Shutdown()
			_ = listener.Close()
		}
	}
}

func (sg *RouterImpl) initJaegerTracer() (io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: "router",
		Sampler: &jaegercfg.SamplerConfig{
			Type:              "const",
			Param:             1,
			SamplingServerURL: config.Get().JaegerConfig.JaegerUrl,
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
		"router",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
}

func (sg *RouterImpl) servAdm(netconn net.Conn) error {

	cl := rrouter.NewClient(netconn)

	if err := cl.Init(sg.frTLS, config.SSLMODEDISABLE); err != nil {
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	return sg.ConsoleDB.Serve(cl)
}

func (sg *RouterImpl) RunAdm(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return errors.Wrap(err, "RunAdm failed")
		}
		go func() {
			if err := sg.servAdm(conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}
