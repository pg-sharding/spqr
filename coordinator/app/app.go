package app

import (
	"net"
	"sync"

	"github.com/pg-sharding/spqr/coordinator"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type App struct {
	c coordinator.Coordinator
}

func NewApp(c coordinator.Coordinator) *App {
	return &App{
		c: c,
	}
}

func (a *App) Run() error {

	tracelog.InfoLogger.Printf("running coordinator app\n")

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(a.ServeGrpc(wg))
	}(wg)
	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(a.ServePsql(wg))
	}(wg)

	wg.Wait()
	tracelog.InfoLogger.Printf("exit")
	return nil
}

func (a *App) ServePsql(wg *sync.WaitGroup) error {

	defer wg.Done()

	tracelog.InfoLogger.Printf("serve psql on localhost 7003")

	listener, err := net.Listen("tcp", "localhost:7003")

	if err != nil {
		return err
	}

	for {
		c, err := listener.Accept()
		tracelog.ErrorLogger.PrintError(err)
		_ = a.c.ProcClient(c)
	}
}

func (a *App) ServeGrpc(wg *sync.WaitGroup) error {

	defer wg.Done()

	tracelog.InfoLogger.Printf("serve grpc on localhost 7002")

	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	//krserv := NewKeyRangeService(d)
	//rrserv := NewRoutersService(d)

	//shards.RegisterKeyRangeServiceServer(serv, krserv)
	//shards.RegisterRoutersServiceServer(serv, rrserv)

	httpAddr := config.RouterConfig().CoordinatorHttpAddr
	httpAddr = "localhost:7002"

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("coordinator listening on %s", httpAddr)

	return serv.Serve(listener)
}
