package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/server"
)

func BindAndReadSliceResult(rst *RelayStateImpl, bind *pgproto3.Bind, portal string) error {

	/* Case when no describe stmt was issued before Execute+Sync*/

	es := &ExecutorState{
		Msg: bind,
		P:   rst.bindQueryPlan, /*  ugh... fix this someday */
	}
	switch rst.bindQueryPlan.(type) {
	case *plan.VirtualPlan:
	default:
		/* this is pretty ugly but lets just do it */
		if err := DispatchPlan(es, rst.Client().Server(), rst.Client(), false); err != nil {
			return err
		}
		if portal == "" {
			/* save extra allocation */
			es.Msg = pgexec
		} else {
			es.Msg = &pgproto3.Execute{
				Portal: portal,
			}
		}

		if err := DispatchPlan(es, rst.Client().Server(), rst.Client(), false); err != nil {
			return err
		}
	}

	es.Msg = pgsync

	replyClient := true

	if err := rst.qse.ExecuteSlicePrepare(es, rst.Qr.Mgr(), replyClient, false); err != nil {
		return err
	}

	return rst.qse.ExecuteSlice(es, rst.Qr.Mgr(), replyClient)
}

func gangMemberDeployPreparedStatement(shard shard.ShardHostInstance, hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {

	shardId := shard.ID()

	if ok, rd := shard.HasPrepareStatement(hash, shardId); ok {
		return rd, &pgproto3.ParseComplete{}, nil
	}

	// Do not wait for result
	// simply fire backend msg
	if err := shard.Send(&pgproto3.Parse{
		Name:          d.Name,
		Query:         d.Query,
		ParameterOIDs: d.ParameterOIDs,
	}); err != nil {
		return nil, nil, err
	}

	if err := shard.Send(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       d.Name,
	}); err != nil {
		return nil, nil, err
	}

	if err := shard.Send(pgsync); err != nil {
		return nil, nil, err
	}

	rd := &prepstatement.PreparedStatementDescriptor{
		NoData:    false,
		RowDesc:   nil,
		ParamDesc: nil,
	}

	var retMsg pgproto3.BackendMessage

	deployResultReceived := false
	deployed := false

recvLoop:
	for {
		msg, err := shard.Receive()
		if err != nil {
			return nil, nil, err
		}

		switch q := msg.(type) {
		case *pgproto3.ParseComplete:
			// skip
			retMsg = msg
			deployResultReceived = true
			deployed = true
		case *pgproto3.ErrorResponse:
			retMsg = msg
			deployResultReceived = true
		case *pgproto3.NoData:
			rd.NoData = true
		case *pgproto3.ParameterDescription:
			// copy
			cp := *q
			rd.ParamDesc = &cp
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
		case *pgproto3.ReadyForQuery:
			break recvLoop
		default:
		}
	}
	if !deployResultReceived {
		return nil, nil, fmt.Errorf("error syncing connection on shard: %v", shardId)
	}

	if deployed {
		if err := shard.StorePrepareStatement(hash, shardId, d, rd); err != nil {
			return nil, nil, err
		}
	}

	return rd, retMsg, nil
}

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

	if bind.DestinationPortal == "" {
		if err := serv.SendShard(portalClose, shkey); err != nil {
			return nil, err
		}
	} else {
		if err := serv.SendShard(&pgproto3.Close{
			ObjectType: 'P',
			Name:       bind.DestinationPortal,
		}, shkey); err != nil {
			return nil, err
		}
	}

	if err := serv.SendShard(pgsync, shkey); err != nil {
		return nil, err
	}

	rd := &PortalDesc{}
	var saveCloseComplete *pgproto3.CloseComplete

recvLoop:
	for {
		// https://www.postgresql.org/docs/current/protocol-flow.html

		msg, err := serv.ReceiveShard(shardId)
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
			rd.nodata = pgNoData
		case *pgproto3.CloseComplete:
			saveCloseComplete = q

		case *pgproto3.RowDescription:
			// copy
			rd.rd = &pgproto3.RowDescription{}

			rd.rd.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

			for i := range len(q.Fields) {
				s := make([]byte, len(q.Fields[i].Name))
				copy(s, q.Fields[i].Name)

				rd.rd.Fields[i] = q.Fields[i]
				rd.rd.Fields[i].Name = s
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
