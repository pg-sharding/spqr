package relay_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	meta_mock "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/qdb"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	relay_mock "github.com/pg-sharding/spqr/router/mock/relay"
	"github.com/pg-sharding/spqr/router/relay"
	"github.com/stretchr/testify/assert"
)

func TestRoutingHint(t *testing.T) {

	assert := assert.New(t)

	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	rst := relay_mock.NewMockRelayStateMgr(ctrl)

	qr := mockqr.NewMockQueryRouter(ctrl)

	rst.EXPECT().QueryRouter().Return(qr)

	emgr := meta_mock.NewMockEntityMgr(ctrl)

	qr.EXPECT().Mgr().Return(emgr)

	dsId := "ds1"

	ds := &distributions.Distribution{
		Id: dsId,
		ColTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}

	emgr.EXPECT().GetDistribution(gomock.Any(), dsId).Return(ds, nil)

	bnd, err := relay.DeparseRouteHintBound(ctx, rst, map[string]string{}, "30", dsId)

	assert.Equal(bnd, []interface{}{int64(30)})

	assert.NoError(err)
}
