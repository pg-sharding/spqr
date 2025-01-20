package plan_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/stretchr/testify/assert"
)

func TestCombineExecutionSlices(t *testing.T) {
	assert := assert.New(t)
	type ttc struct {
		p1  []*kr.ShardKey
		p2  []*kr.ShardKey
		exp []*kr.ShardKey
	}

	for _, tt := range []ttc{
		{
			p1:  nil,
			p2:  nil,
			exp: nil,
		},
		{
			p1: nil,
			p2: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
			exp: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
		},
		{
			p1: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
			p2: nil,
			exp: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
		},
		{
			p1: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
			p2: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
			exp: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
		},
		{
			p1: []*kr.ShardKey{
				{
					Name: "n1",
				},
			},
			p2: []*kr.ShardKey{
				{
					Name: "n2",
				},
			},
			exp: []*kr.ShardKey{
				{
					Name: "n1",
				},
				{
					Name: "n2",
				},
			},
		},
	} {

		res := plan.CombineExecutionTargets(tt.p1, tt.p2)

		assert.Equal(tt.exp, res)
	}
}
