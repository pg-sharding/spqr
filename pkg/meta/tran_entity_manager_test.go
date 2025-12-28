package meta_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	distributions "github.com/pg-sharding/spqr/pkg/models/distributions"
)

func TestTranEntitySaveBefore(t *testing.T) {
	is := assert.New(t)
	list := meta.NewMetaEntityList[*distributions.Distribution]()
	list.Save("ds1", &distributions.Distribution{Id: "ds1", ColTypes: []string{"integer"}})
	list.Save("ds2", &distributions.Distribution{Id: "ds2", ColTypes: []string{"integer"}})
	expectedExist := map[string]*distributions.Distribution{"ds1": {Id: "ds1", ColTypes: []string{"integer"}}, "ds2": {Id: "ds2", ColTypes: []string{"integer"}}}
	expectedDeleted := map[string]struct{}{}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())

	list.Delete("ds1")
	expectedExist = map[string]*distributions.Distribution{"ds2": {Id: "ds2", ColTypes: []string{"integer"}}}
	expectedDeleted = map[string]struct{}{"ds1": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())
}
func TestTranEntityDelBefore(t *testing.T) {
	is := assert.New(t)
	list := meta.NewMetaEntityList[*distributions.Distribution]()
	list.Delete("ds1")
	list.Delete("ds3")
	expectedExist := map[string]*distributions.Distribution{}
	expectedDeleted := map[string]struct{}{"ds1": {}, "ds3": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())

	list.Save("ds3", &distributions.Distribution{Id: "ds3", ColTypes: []string{"integer"}})
	expectedExist = map[string]*distributions.Distribution{"ds3": {Id: "ds3", ColTypes: []string{"integer"}}}
	expectedDeleted = map[string]struct{}{"ds1": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())
}

func TestTranGetDistribution(t *testing.T) {
	is := assert.New(t)

	t.Run("test with save changes", func(t *testing.T) {
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)
		ds0 := distributions.NewDistribution("ds0", []string{"integer"})
		chunk, err := mngr.CreateDistribution(ctx, ds0)
		is.NoError(err)
		err = mngr.ExecNoTran(ctx, chunk)
		is.NoError(err)

		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		ds2 := distributions.NewDistribution("ds2", []string{"integer"})
		_, err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		//NO COMMIT QDB!!!
		_, err = tranMngr.CreateDistribution(ctx, ds2)
		is.NoError(err)
		//NO COMMIT QDB!!!

		//check List
		actualList, err := tranMngr.ListDistributions(ctx)
		is.NoError(err)
		is.Len(actualList, 3)
		is.Equal(map[string]*distributions.Distribution{"ds1": ds1, "ds2": ds2, "ds0": ds0},
			map[string]*distributions.Distribution{actualList[0].Id: actualList[0], actualList[1].Id: actualList[1], actualList[2].Id: actualList[2]})

		//check Get
		actual1, err := tranMngr.GetDistribution(ctx, "ds1")
		is.NoError(err)
		is.Equal(ds1, actual1)
		actual0, err := tranMngr.GetDistribution(ctx, "ds0")
		is.NoError(err)
		is.Equal(ds0, actual0)
		_, err = tranMngr.GetDistribution(ctx, "ds-1")
		is.EqualError(err, "distribution \"ds-1\" not found")
	})

	t.Run("test with delete changes", func(t *testing.T) {
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)
		ds0 := distributions.NewDistribution("ds0", []string{"integer"})
		chunk, err := mngr.CreateDistribution(ctx, ds0)
		is.NoError(err)
		err = mngr.ExecNoTran(ctx, chunk)
		is.NoError(err)

		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		ds2 := distributions.NewDistribution("ds2", []string{"integer"})
		_, err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		//NO COMMIT QDB!!!
		_, err = tranMngr.CreateDistribution(ctx, ds2)
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.DropDistribution(ctx, "ds2")
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.DropDistribution(ctx, "ds0")
		is.NoError(err)
		//NO COMMIT QDB!!!

		//check List
		actualList, err := tranMngr.ListDistributions(ctx)
		is.NoError(err)
		is.Equal([]*distributions.Distribution{ds1}, actualList)

		//check Get
		_, err = tranMngr.GetDistribution(ctx, "ds2")
		is.EqualError(err, "distribution \"ds2\" not found")
		_, err = tranMngr.GetDistribution(ctx, "ds0")
		is.EqualError(err, "distribution \"ds0\" not found")

		actualQdb, err := mngr.GetDistribution(ctx, "ds0")
		is.NoError(err)
		is.Equal(ds0, actualQdb)
	})

}
