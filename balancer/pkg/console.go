package pkg

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgproto3"

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
			spqrlog.Zero.Fatal().Err(err)
		}
	}

	spqrlog.Zero.Info().
		Msg("console.ProcClient start")

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
		case *pgproto3.Terminate:
			return nil
		default:
			spqrlog.Zero.Info().
				Type("message-type", v).
				Msg("got unexpected postgresql proto message")
		}
	}
}

func (c *Console) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	cli := clientinteractor.NewPSQLInteractor(cl)
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		return err
	}

	spqrlog.Zero.Info().
		Str("query", q).
		Type("statement-type", tstmt).
		Msg("process query")

	switch stmt := tstmt.(type) {
	case *spqrparser.Show:
		spqrlog.Zero.Info().
			Str("cmd", stmt.Cmd).
			Msg("parsed statement is")

		switch stmt.Cmd {
		case spqrparser.KeyRangesStr:
			keyRanges, err := c.console.showKeyRanges()
			if err != nil {
				spqrlog.Zero.Error().
					Err(err).
					Str("client", cl.ID()).
					Msg("failed to show key ranges")
			}

			return cli.KeyRanges(keyRanges)

		default:
			spqrlog.Zero.Error().
				Str("cmd", stmt.Cmd).
				Err(err).
				Msg("unknown stmt.Cmd")

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
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("border", border).
				Msg("failed to split key range by border")
		}

		return cli.SplitKeyRange(ctx, split)

	case *spqrparser.UniteKeyRange:
		unite := &kr.UniteKeyRange{
			KeyRangeIDLeft:  stmt.KeyRangeIDL,
			KeyRangeIDRight: stmt.KeyRangeIDR,
		}

		// Get key range border
		border := stmt.KeyRangeIDR
		err := c.coord.mergeKeyRanges(&border)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("left", stmt.KeyRangeIDL).
				Str("right", stmt.KeyRangeIDR).
				Msg("failed to merge key ranges")
		}

		return cli.MergeKeyRanges(ctx, unite)

	case *spqrparser.MoveKeyRange:
		// Get key range border by stmt.KeyRangeID
		keyRangeBorders := KeyRange{
			left:  "",
			right: "",
		}

		shardID, err := strconv.Atoi(stmt.DestShardID)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("dest-shard-id", stmt.DestShardID).
				Msg("failed to detect shard")
		}

		err = c.coord.moveKeyRange(keyRangeBorders, Shard{id: shardID})
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("key-range", stmt.KeyRangeID).
				Str("dest-shard", stmt.DestShardID).
				Msg("failed to move key range")
		}

		moveKeyRange := &kr.MoveKeyRange{Krid: stmt.KeyRangeID, ShardId: stmt.DestShardID}

		return cli.MoveKeyRange(ctx, moveKeyRange)

	case *spqrparser.Lock:
		// TODO: get key range by ID.
		keyRange := KeyRange{}
		err := c.coord.lockKeyRange(keyRange)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("key-range", stmt.KeyRangeID).
				Msg("failed to lock key range")
		}

		return cli.LockKeyRange(ctx, stmt.KeyRangeID)

	case *spqrparser.Unlock:
		// TODO: get key range by ID.
		keyRange := KeyRange{}
		err := c.coord.unlockKeyRange(keyRange)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Str("client", cl.ID()).
				Str("key-range", stmt.KeyRangeID).
				Msg("failed to unlock key range")
		}

		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID)

	case *spqrparser.Shutdown:
		//t.stchan <- struct{}{}
		return fmt.Errorf("not implemented")

	default:
		spqrlog.Zero.Error().
			Type("type", tstmt).
			Interface("tstmt", tstmt).
			Msg("got unexcepted console request")
		if err := cl.DefaultReply(); err != nil {
			spqrlog.Zero.Fatal().Err(err).Msg("")
		}
	}

	return nil
}

func (c *Console) Shutdown() error {
	return nil
}
