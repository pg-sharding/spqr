package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/router/server"
)

func sliceDescribePortal(serv server.Server, portalDesc *pgproto3.Describe, bind *pgproto3.Bind) (*prepstatement.PreparedStatementDescriptor, error) {

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

	if err := serv.SendShard(&pgproto3.Sync{}, shkey); err != nil {
		return nil, err
	}

	rd := &prepstatement.PreparedStatementDescriptor{
		NoData:    false,
		RowDesc:   nil,
		ParamDesc: nil,
	}
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
			rd.NoData = true
		case *pgproto3.CloseComplete:
			saveCloseComplete = q

		case *pgproto3.RowDescription:
			// copy
			rd.RowDesc = &pgproto3.RowDescription{}

			rd.RowDesc.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

			for i := range len(q.Fields) {
				s := make([]byte, len(q.Fields[i].Name))
				copy(s, q.Fields[i].Name)

				rd.RowDesc.Fields[i] = q.Fields[i]
				rd.RowDesc.Fields[i].Name = s
			}
		default:
			return nil, fmt.Errorf("received unexpected message type %T", msg)
		}
	}

	if saveCloseComplete == nil {
		return nil, fmt.Errorf("portal was not closed after describe")
	}

	return rd, nil
}
