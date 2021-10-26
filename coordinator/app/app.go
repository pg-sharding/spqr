package app

import (
	"github.com/pg-sharding/spqr/coordinator"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"sync"
)


type App struct {
	c coordinator.Coordinator
}

func NewApp(c coordinator.Coordinator) *App{
	return &App{
		c: c,
	}
}

func (a *App) Run() error {

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go a.ServeGrpc(wg)
	go a.ServePsql(wg)

	wg.Wait()
	return nil
}

func (a *App) ServePsql(wg *sync.WaitGroup) error {

	wg.Done()

	listener, err := net.Listen("tcp", "localhost:7003")

	if err != nil {
		return err
	}

	for {
		c, err := listener.Accept()
		tracelog.ErrorLogger.PrintError(err)
		_ = a.c.Serve(c)
	}
}


func (a *App) ServeGrpc(wg *sync.WaitGroup) error {


	wg.Done()

	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	//krserv := NewKeyRangeService(d)
	//rrserv := NewRoutersService(d)

	//shards.RegisterKeyRangeServiceServer(serv, krserv)
	//shards.RegisterRoutersServiceServer(serv, rrserv)

	httpAddr := config.Get().CoordinatorHttpAddr
	httpAddr = "localhost:7002"

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("coordinator listening on %s", httpAddr)

	return serv.Serve(listener)
}
