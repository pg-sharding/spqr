package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/server"
)

func sliceDescribePortal(serv server.Server, portalDesc *pgproto3.Describe, bind *pgproto3.Bind) (*PortalDesc, error) {

	shards := serv.Datashards()
	if len(shards) == 0 {
		return nil, spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "No active shards")
	}

	shard := shards[0]
	shardId := shard.ID()
	shkey := shard.SHKey()

	if err := serv.SendShard(bind, shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(portalDesc, shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(portalClose, shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(pgsync, shkey); err != nil {
		return nil, err
	}

	pd := &PortalDesc{}
	var saveCloseComplete *pgproto3.CloseComplete

recvLoop:
	for {
		// https://www.postgresql.org/docs/current/protocol-flow.html

		msg, _, err := serv.ReceiveShard(shardId)
		if err != nil {
			return nil, err
		}
		switch q := msg.(type) {
		case *pgproto3.BindComplete:
			// that's ok
		case *pgproto3.ReadyForQuery:
			break recvLoop
		case *pgproto3.ErrorResponse:
			return nil, fmt.Errorf("error describing slice portal: \"%s\"", q.Message)
		case *pgproto3.NoData:
			pd.nodata = pgNoData
		case *pgproto3.CloseComplete:
			saveCloseComplete = q

		case *pgproto3.RowDescription:
			// copy
			pd.rd = &pgproto3.RowDescription{}

			pd.rd.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

			for i := range len(q.Fields) {
				s := make([]byte, len(q.Fields[i].Name))
				copy(s, q.Fields[i].Name)

				pd.rd.Fields[i] = q.Fields[i]
				pd.rd.Fields[i].Name = s
			}
		default:
			return nil, fmt.Errorf("received unexpected message type %T", msg)
		}
	}

	if saveCloseComplete == nil {
		return nil, fmt.Errorf("portal was not closed after describe")
	}

	return pd, nil
}
