package pkg

import (
	"context"
	"crypto/tls"

	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/client"
)

type Console struct {
	cfg    *tls.Config
	stchan chan struct{}
}

func NewConsole(cfg *tls.Config, stchan chan struct{}) (*Console, error) {
	return &Console{
		cfg:    cfg,
		stchan: stchan,
	}, nil
}

func (c *Console) Serve(ctx context.Context, cl client.Client) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.NoticeResponse{
			Message: "Welcome",
		},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	tracelog.InfoLogger.Print("console.ProcClient start")

	for {
		msg, err := cl.Receive()

		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			if err := c.ProcessQuery(ctx, v.String, cl); err != nil {
				_ = cl.ReplyErrMsg(err.Error())
				// continue to consume input
			}
		default:
			tracelog.InfoLogger.Printf("got unexpected postgresql proto message with type %T", v)
		}
	}
}

func (c *Console) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	return nil
}

func (c *Console) Shutdown() error {
	return nil
}
