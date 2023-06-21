package pkg

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
)

type CoordinatorInterface interface {
	initKeyRanges() (map[Shard][]KeyRange, error)
	isReloadRequired() (bool, error)

	lockKeyRange(rng KeyRange) error
	unlockKeyRange(rng KeyRange) error

	splitKeyRange(border *string, krID, sourceID string) error
	mergeKeyRanges(border *string) error
	moveKeyRange(rng KeyRange, shardTo Shard) error
}

type ConsoleInterface interface {
	showKeyRanges() ([]*kr.KeyRange, error)
}

type Coordinator struct {
	maxRetriesCount        int
	addr                   string
	balancerServiceClient  routerproto.BalancerServiceClient
	shardServiceClient     routerproto.ShardServiceClient
	keyRangeServiceClient  routerproto.KeyRangeServiceClient
	operationServiceClient routerproto.OperationServiceClient
}

func (c *Coordinator) showKeyRanges() ([]*kr.KeyRange, error) {
	respList, err := c.keyRangeServiceClient.ListKeyRange(context.Background(), &routerproto.ListKeyRangeRequest{})
	if err != nil {
		return nil, err
	}

	res := make([]*kr.KeyRange, 0, len(respList.KeyRangesInfo))
	for _, keyRangeInfo := range respList.KeyRangesInfo {
		keyRange := &kr.KeyRange{
			LowerBound: []byte(keyRangeInfo.GetKeyRange().GetLowerBound()),
			UpperBound: []byte(keyRangeInfo.GetKeyRange().GetUpperBound()),
			ShardID:    keyRangeInfo.GetShardId(),
			ID:         keyRangeInfo.GetKrid(),
		}

		res = append(res, keyRange)
	}

	return res, nil
}

func (c *Coordinator) Init(addr string, maxRetriesCount int) error {
	c.addr = addr
	c.maxRetriesCount = maxRetriesCount
	connect, err := grpc.Dial(addr)
	if err != nil {
		return err
	}
	c.balancerServiceClient = routerproto.NewBalancerServiceClient(connect)

	connect, err = grpc.Dial(addr)
	if err != nil {
		return err
	}
	c.shardServiceClient = routerproto.NewShardServiceClient(connect)

	connect, err = grpc.Dial(addr)
	if err != nil {
		return err
	}
	c.keyRangeServiceClient = routerproto.NewKeyRangeServiceClient(connect)

	connect, err = grpc.Dial(addr)
	if err != nil {
		return err
	}
	c.operationServiceClient = routerproto.NewOperationServiceClient(connect)
	return nil
}

func (c *Coordinator) ShardsList() (*map[int]routerproto.ShardInfo, error) {
	respList, err := c.shardServiceClient.ListShards(context.Background(), &routerproto.ListShardsRequest{})
	if err != nil {
		return nil, err
	}

	res := map[int]routerproto.ShardInfo{}
	for _, shard := range respList.Shards {
		respShard, err := c.shardServiceClient.GetShardInfo(context.Background(), &routerproto.ShardRequest{
			Id: shard.Id,
		})
		if err != nil {
			return nil, err
		}
		id, err := strconv.Atoi(shard.Id)
		if err != nil {
			return nil, err
		}

		res[id] = routerproto.ShardInfo{Hosts: respShard.ShardInfo.Hosts}
	}

	return &res, nil
}

func (c *Coordinator) waitTilDone(operationID string) error {
	// TODO: skip waiting because the operation service is not implemented.
	time.Sleep(time.Second)
	return nil
}

func (c *Coordinator) initKeyRanges() (map[Shard][]KeyRange, error) {
	resp, err := c.keyRangeServiceClient.ListKeyRange(context.Background(), &routerproto.ListKeyRangeRequest{})
	if err != nil {
		return nil, err
	}

	res := map[Shard][]KeyRange{}
	for _, kr := range resp.KeyRangesInfo {
		id, err := strconv.Atoi(kr.ShardId)
		if err != nil {
			return nil, err
		}
		shard := Shard{id: id}
		_, ok := res[shard]
		if !ok {
			res[shard] = []KeyRange{}
		}
		res[shard] = append(res[shard], KeyRange{left: kr.KeyRange.LowerBound, right: kr.KeyRange.UpperBound})
	}

	return res, nil
}

func (c *Coordinator) isReloadRequired() (bool, error) {
	// resp, err := c.balancerServiceClient.ReloadRequired(context.Background(), &routerproto.ReloadRequest{})
	// if err != nil {
	// 	return false, err
	// }

	// return resp.ReloadRequired, nil

	return false, nil //TODO: temporary skip: the ReloadRequired method is not implemented.
}

func (c *Coordinator) lockKeyRange(rng KeyRange) error {
	return nil
	// TODO: resolve key range by bound
	//resp, err := c.keyRangeServiceClient.LockKeyRange(context.Background(), &routerproto.LockKeyRangeRequest{
	//	KeyRange: &routerproto.KeyRangeInfo{KeyRange: &routerproto.KeyRange{LowerBound: rng.left, UpperBound: rng.right}},
	//})
	//if err != nil {
	//	return err
	//}
	//return c.waitTilDone(resp.OperationId)
}

func (c *Coordinator) unlockKeyRange(rng KeyRange) error {
	return nil
	//resp, err := c.keyRangeServiceClient.UnlockKeyRange(context.Background(), &routerproto.UnlockKeyRangeRequest{
	//	KeyRange: &routerproto.KeyRangeInfo{KeyRange: &routerproto.KeyRange{LowerBound: rng.left, UpperBound: rng.right}},
	//})
	//if err != nil {
	//	return err
	//}
	//return c.waitTilDone(resp.OperationId)
}

func (c *Coordinator) splitKeyRange(border *string, krID, sourceID string) error {
	resp, err := c.keyRangeServiceClient.SplitKeyRange(context.Background(), &routerproto.SplitKeyRangeRequest{
		Bound: []byte(*border),
		KeyRangeInfo: &routerproto.KeyRangeInfo{
			Krid: krID,
		},
		SourceId: sourceID,
	})
	if err != nil {
		return err
	}
	return c.waitTilDone(resp.OperationId)
}

func (c *Coordinator) mergeKeyRanges(border *string) error {
	resp, err := c.keyRangeServiceClient.MergeKeyRange(context.Background(), &routerproto.MergeKeyRangeRequest{
		Bound: []byte(*border),
	})
	if err != nil {
		return err
	}
	return c.waitTilDone(resp.OperationId)
}

func (c *Coordinator) moveKeyRange(rng KeyRange, shardTo Shard) error {
	resp, err := c.keyRangeServiceClient.MoveKeyRange(context.Background(), &routerproto.MoveKeyRangeRequest{
		KeyRange:  &routerproto.KeyRangeInfo{KeyRange: &routerproto.KeyRange{LowerBound: rng.left, UpperBound: rng.right}},
		ToShardId: strconv.Itoa(shardTo.id),
	})
	if err != nil {
		return err
	}
	return c.waitTilDone(resp.OperationId)
}
