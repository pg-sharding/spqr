package pkg

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type Console struct {
	cfg     *tls.Config
	coord   CoordinatorInterface
	console ConsoleInterface
	stchan  chan struct{}
}

func NewConsole(cfg *tls.Config, coord CoordinatorInterface, co ConsoleInterface, stchan chan struct{}) (*Console, error) {
	return &Console{
		cfg:     cfg,
		coord:   coord,
		console: co,
		stchan:  stchan,
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
			spqrlog.Logger.FatalOnError(err)
		}
	}

	spqrlog.Logger.Printf(spqrlog.INFO, "console.ProcClient start")

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
			spqrlog.Logger.Printf(spqrlog.INFO, "got unexpected postgresql proto message with type %T", v)
		}
	}
}

func (c *Console) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	cli := clientinteractor.PSQLInteractor{}
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.INFO, "RouterConfig '%s', parsed %T", q, tstmt)

	switch stmt := tstmt.(type) {
	case *spqrparser.Show:

		spqrlog.Logger.Printf(spqrlog.INFO, "parsed %s", stmt.Cmd)

		switch stmt.Cmd {
		case spqrparser.ShowKeyRangesStr:
			keyRanges, err := c.console.showKeyRanges()
			if err != nil {
				spqrlog.Logger.Errorf("failed to show key ranges: %w", err)
			}

			return cli.KeyRanges(keyRanges, cl)

		default:
			spqrlog.Logger.Printf(spqrlog.INFO, "Unknown default %s", stmt.Cmd)

			return fmt.Errorf("Unknown show statement: %s", stmt.Cmd)
		}

	case *spqrparser.SplitKeyRange:
		split := &kr.SplitKeyRange{
			Bound:    stmt.Border,
			Krid:     stmt.KeyRangeID,
			SourceID: stmt.KeyRangeFromID,
		}
		border := string(stmt.Border)
		err := c.coord.splitKeyRange(&border, split.Krid, stmt.KeyRangeFromID)
		if err != nil {
			spqrlog.Logger.Errorf("failed to split key range by border %s: %#v", border, err)
		}

		return cli.SplitKeyRange(ctx, split, cl)

	case *spqrparser.UniteKeyRange:
		unite := &kr.UniteKeyRange{
			KeyRangeIDLeft:  stmt.KeyRangeIDL,
			KeyRangeIDRight: stmt.KeyRangeIDR,
		}

		// Get key range border
		border := stmt.KeyRangeIDR
		err := c.coord.mergeKeyRanges(&border)
		if err != nil {
			spqrlog.Logger.Errorf("failed to merge key ranges %s and %s: %#v", stmt.KeyRangeIDL, stmt.KeyRangeIDR, err)
		}

		return cli.MergeKeyRanges(ctx, unite, cl)

	case *spqrparser.MoveKeyRange:
		// Get key range border by stmt.KeyRangeID
		keyRangeBorders := KeyRange{
			left:  "",
			right: "",
		}

		shardID, err := strconv.Atoi(stmt.DestShardID)
		if err != nil {
			spqrlog.Logger.Errorf("failed to detect shard ID: %#v", stmt.DestShardID, err)
		}

		err = c.coord.moveKeyRange(keyRangeBorders, Shard{id: shardID})
		if err != nil {
			spqrlog.Logger.Errorf("failed to move key range %s to shard %v: %#v", stmt.KeyRangeID, stmt.DestShardID, err)
		}

		moveKeyRange := &kr.MoveKeyRange{Krid: stmt.KeyRangeID, ShardId: stmt.DestShardID}

		return cli.MoveKeyRange(ctx, moveKeyRange, cl)

	case *spqrparser.Lock:
		// TODO: get key range by ID.
		keyRange := KeyRange{}
		err := c.coord.lockKeyRange(keyRange)
		if err != nil {
			spqrlog.Logger.Errorf("failed to lock key range %s: %#v", stmt.KeyRangeID, err)
		}

		return cli.LockKeyRange(ctx, stmt.KeyRangeID, cl)

	case *spqrparser.Unlock:
		// TODO: get key range by ID.
		keyRange := KeyRange{}
		err := c.coord.unlockKeyRange(keyRange)
		if err != nil {
			spqrlog.Logger.Errorf("failed to unlock key range %s: %#v", stmt.KeyRangeID, err)
		}

		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID, cl)

	case *spqrparser.Shutdown:
		//t.stchan <- struct{}{}
		return fmt.Errorf("not implemented")

	default:
		spqrlog.Logger.Printf(spqrlog.INFO, "got unexcepted console request %v %T", tstmt, tstmt)
		if err := cl.DefaultReply(); err != nil {
			spqrlog.Logger.FatalOnError(err)
		}
	}

	return nil
}

func (c *Console) Shutdown() error {
	return nil
}
