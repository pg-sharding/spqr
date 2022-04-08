package app

import (
	"context"
	"net"
	"sync"

	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pg-sharding/spqr/coordinator"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
)

type App struct {
	coordiantor coordinator.Coordinator
}

func NewApp(c coordinator.Coordinator) *App {
	return &App{
		coordiantor: c,
	}
}

func (app *App) Run() error {

	tracelog.InfoLogger.Printf("running coordinator app\n")

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(app.ServeGrpc(wg))
	}(wg)
	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(app.ServePsql(wg))
	}(wg)

	wg.Wait()
	tracelog.InfoLogger.Printf("exit")
	return nil
}

func (app *App) ServePsql(wg *sync.WaitGroup) error {

	defer wg.Done()

	tracelog.InfoLogger.Printf("serve psql on %v", config.CoordinatorConfig().Addr)

	listener, err := net.Listen("tcp", config.CoordinatorConfig().Addr)

	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		tracelog.ErrorLogger.PrintError(err)
		_ = app.coordiantor.ProcClient(context.TODO(), conn)
	}
}

func (app *App) ServeGrpc(wg *sync.WaitGroup) error {

	defer wg.Done()

	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	//krserv := NewKeyRangeService(d)
	//rrserv := NewRoutersService(d)

	//shards.RegisterKeyRangeServiceServer(serv, krserv)
	//shards.RegisterRoutersServiceServer(serv, rrserv)

	httpAddr := config.CoordinatorConfig().HttpAddr

	tracelog.InfoLogger.Printf("serve grpc on %v", httpAddr)

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("coordinator listening on %s", httpAddr)

	return serv.Serve(listener)
}
