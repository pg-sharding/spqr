package qrouter

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type LocalQrouter struct {
	QueryRouter
	shid string
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(shid string) (*LocalQrouter, error) {
	return &LocalQrouter{
		shid: shid,
	}, nil
}

func (l *LocalQrouter) Route(q string) (RoutingState, error) {
	return ShardMatchState{
		Routes: []*ShardRoute{
			{
				Shkey: kr.ShardKey{
					Name: l.shid,
				},
			},
		},
	}, nil
}
