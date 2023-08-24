package app_test

import (
	"io"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	app "github.com/pg-sharding/spqr/router"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"

	mockcmgr "github.com/pg-sharding/spqr/router/mock/rulerouter"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestFrontendSimpleEOF(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")
	cl.EXPECT().Close().Times(1)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).Times(1)

	err := app.Frontend(qr, cl, cmngr, &config.Router{})

	assert.NoError(err, "")
}
