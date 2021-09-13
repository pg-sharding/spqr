package internal

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"github.com/opentracing/opentracing-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/console"
	"github.com/pg-sharding/spqr/internal/qdb"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pg-sharding/spqr/internal/rrouter"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type Spqr struct {
	Router  rrouter.Router
	Qrouter qrouter.Qrouter
	ConsoleDB console.Console
	SPIexecuter *Executer
	stchan chan struct{}
	frTLS *tls.Config
}

func NewSpqr(dataFolder string) (*Spqr, error) {
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

	return &Spqr{
		Router:  rr,
		Qrouter: qr,
		ConsoleDB: cnsl,
		SPIexecuter: executer,
		stchan:  stchan,
		frTLS:   frTLS,
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

func (sg *Spqr) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := rrouter.InitClConnection(client)
	if err != nil {
		return err
	}

	return frontend(sg.Qrouter, client, cmngr)
}

func (sg *Spqr) Run(listener net.Listener) error {
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
			_ = sg.Router.Shutdown()
			_ = listener.Close()
		}
	}
}

func (sg *Spqr) initJaegerTracer() (io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: "spqr",
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
		"spqr",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
}

func (sg *Spqr) servAdm(netconn net.Conn) error {

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

func (sg *Spqr) RunAdm(listener net.Listener) error {
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
