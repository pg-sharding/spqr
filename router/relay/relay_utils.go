package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/spaolacci/murmur3"
)

func BindAndReadSliceResult(rst *RelayStateImpl, p plan.Plan, forceSimple bool, bind *pgproto3.Bind, portal string, maxrows uint32, preparedStatements map[string]string) error {

	/* Case when no describe stmt was issued before Execute+Sync*/

	qd := &QueryDesc{
		Msg:                bind,
		ParamsNum:          len(bind.Parameters),
		preparedStatements: preparedStatements,
		simple:             forceSimple,
	}

	if portal == "" {
		/* save extra allocation */
		if maxrows == 0 {
			qd.exec = pgexec
		} else {
			qd.exec = &pgproto3.Execute{
				MaxRows: maxrows,
			}
		}
	} else {
		qd.exec = &pgproto3.Execute{
			Portal:  portal,
			MaxRows: maxrows,
		}
	}

	return rst.QueryExecutor().ExecuteSlice(qd, p, true)
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
			rd.RowDesc = &pgproto3.RowDescription{
				Fields: xproto.CopyFieldDescriptions(q.Fields),
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
	if serv == nil {
		return nil, fmt.Errorf("failed to describe portal for unrouted client")
	}

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
			rd.rd = &pgproto3.RowDescription{
				Fields: xproto.CopyFieldDescriptions(q.Fields),
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

func (rst *RelayStateImpl) deployRewrittenPreparedStatements(
	p plan.Plan,
	originalHash uint64,
	original *prepstatement.PreparedStatementDescriptor,
) (map[string]string, error) {
	scatter, ok := p.(*plan.ScatterPlan)
	if !ok || len(scatter.OverwriteQuery) == 0 {
		return nil, nil
	}
	if original == nil || original.ParamDesc == nil {
		return nil, spqrerror.New(spqrerror.SPQR_UNEXPECTED, "missing parameter types for rewritten query")
	}

	names := make(map[string]string, len(scatter.OverwriteQuery))
	for _, shard := range rst.Client().Server().Datashards() {
		shardName := shard.SHKey().Name
		query, ok := scatter.OverwriteQuery[shardName]
		if !ok {
			continue
		}

		hash := murmur3.Sum64([]byte(fmt.Sprintf("%d:%s", originalHash, query)))
		name := fmt.Sprintf("spqr_rewrite_%d", hash)
		_, msg, err := gangMemberDeployPreparedStatement(shard, hash, &prepstatement.PreparedStatementDefinition{
			Name:          name,
			Query:         query,
			ParameterOIDs: original.ParamDesc.ParameterOIDs,
		})
		if err != nil {
			return nil, err
		}
		if response, ok := msg.(*pgproto3.ErrorResponse); ok {
			return nil, spqrerror.New(response.Code, response.Message)
		}
		names[shardName] = name
	}
	if len(names) != len(scatter.OverwriteQuery) {
		return nil, spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "missing shard for rewritten query")
	}
	return names, nil
}
