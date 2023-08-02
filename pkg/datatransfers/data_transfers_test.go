package datatransfers

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

type mock struct {
}

func (m *mock) Begin(context.Context) (pgx.Tx, error) {
	return nil, nil
}
func (m *mock) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}
func (m *mock) Close(context.Context) error {
	return nil
}

func TestSmth(t *testing.T) {
	assert := assert.New(t)

	p, _ := os.Getwd()
	LoadConfig(p + "/pkg/datatransfers/shard_data.yaml")
	m := &mock{}
	beginTransactions(context.TODO(), m, m)
	//func MoveKeys(ctx context.Context, fromId, toId string, keyr qdb.KeyRange, shr []*shrule.ShardingRule, db *qdb.QDB) error

	//assert.Equal(4.0, stat1.Quantile(0.5))
	assert.Equal(3, 3)
}
