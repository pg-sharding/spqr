package qrouter_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord/local"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/routingstate"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/lyx/lyx"
)

const MemQDBPath = "memqdb.json"

func TestMultiShardRouting(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "ds1"
	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"xx": {
				Name: "xx",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "create table xx (i int);",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "DROP TABLE copy_test;",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "select 42;",
			exp:   routingstate.RandomMatchState{},
			err:   nil,
		},
		{
			query: "select current_schema;",
			exp:   routingstate.RandomMatchState{},
			err:   nil,
		},
		{
			query: "select current_schema();",
			exp:   routingstate.RandomMatchState{},
			err:   nil,
		},
		{
			query: "alter table xx  add column i int;",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "vacuum xx;",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "analyze xx;",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "cluster xx;",
			exp:   routingstate.DDLState{},
			err:   nil,
		},
		{
			query: "SELECT * FROM pg_catalog.pg_type",
			exp:   routingstate.RandomMatchState{},
			err:   nil,
		},

		{
			query: "SELECT * FROM pg_class",
			exp:   routingstate.RandomMatchState{},
			err:   nil,
		},
		{
			query: `SELECT count(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA() AND table_name = 'people' AND table_type = 'BASE TABLE'`,
			exp:   routingstate.RandomMatchState{},
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestComment(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID: distribution,
		ColTypes: []string{
			qdb.ColumnTypeInteger,
		},
		Relations: map[string]*qdb.DistributedRelation{
			"xx": {
				Name: "xx",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		LowerBound: kr.KeyRangeBound{
			int64(1),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		LowerBound: kr.KeyRangeBound{
			int64(11),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "select /* oiwejow--23**/ * from  xx where i = 4;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCTE(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID: distribution,

		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"t": {
				Name: "t",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1", LowerBound: kr.KeyRangeBound{
			int64(1),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2", LowerBound: kr.KeyRangeBound{
			int64(11),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: `WITH qqq AS (
				
			  DELETE FROM t
			  WHERE i = 10 and (k, j) IN (
			  (12::int, 14))
			  )

			  SELECT * FROM qqq;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},

						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
		},

		{
			query: `
			WITH xxxx AS (
				SELECT * from t where i = 1
			)
			SELECT * from xxxx;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
		},
		{
			query: `
			WITH xxxx AS (
				SELECT * from t where i = 1
			),
			zzzz AS (
				UPDATE t 
				SET a = 0
				WHERE i = 1 AND (SELECT COUNT(*) FROM xxxx WHERE b = 0) = 1
			)	
			SELECT * FROM xxxx;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
		},
		// {
		// 	query: `
		// 	WITH xxxx AS (
		// 		SELECT * from t where i = 1
		// 	),
		// 	zzzz AS (
		// 		UPDATE t
		// 		SET a = 0
		// 		WHERE i = 12
		// 	)
		// 	SELECT * FROM xxxx;
		// 	`,
		// 	err: nil,
		// 	exp: routingstate.SkipRoutingState{},
		// },
		{
			query: `
			WITH xxxx AS (
				SELECT * from t where i = 1
			),
			zzzz AS (
				UPDATE t
				SET a = 0
				WHERE i = 2
			)
			SELECT * FROM xxxx;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound:   []interface{}{int64(1)},

						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestSingleShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID: distribution,
		ColTypes: []string{
			qdb.ColumnTypeInteger,
		},
		Relations: map[string]*qdb.DistributedRelation{
			"t": {
				Name: "t",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"yy": {
				Name: "yy",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"xxtt1": {
				Name: "xxtt1",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"xx": {
				Name: "xx",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"xxmixed": {
				Name: "xxmixed",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		LowerBound: kr.KeyRangeBound{
			int64(1),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		LowerBound: kr.KeyRangeBound{
			int64(11),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeInteger,
		},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "SELECT * FROM xxtt1 a WHERE a.i = 21 and w_idj + w_idi != 0;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						Distribution: distribution,
						ID:           "id2",
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxtt1 a WHERE a.i = '21' and w_idj + w_idi != 0;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						Distribution: distribution,
						ID:           "id2",
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: `
			DELETE
				FROM t
			WHERE
				j =
				any(array(select * from t where i <= 2))
			/* __spqr__default_route_behaviour: BLOCK */  returning *;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{

							int64(1),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
		},

		{
			query: `
			DELETE
				FROM t
			WHERE
				i =
				any(array(select * from t where i <= 2))
			/* __spqr__default_route_behaviour: BLOCK */  returning *;
			`,
			err: nil,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{

							int64(1),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
		},
		{
			query: "select * from  xx where i = 4;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "INSERT INTO xx (i) SELECT 20;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: "select * from  xx where i = 11;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "Insert into xx (i) values (1), (2)",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		/* TODO: same query but without alias should work:
		* Insert into xx (i) select * from yy where i = 8
		 */
		{
			query: "Insert into xx (i) select * from yy a where a.i = 8",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(1),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxmixed WHERE i BETWEEN 22 AND 30 ORDER BY id;;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM t WHERE i = 12 AND j = 1;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound:   []interface{}{int64(11)},

						ColumnTypes: []string{
							qdb.ColumnTypeInteger,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: "SELECT * FROM t WHERE i = 12 UNION ALL SELECT * FROM xxmixed WHERE i = 22;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,

						LowerBound: []interface{}{
							int64(11),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestInsertOffsets(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"xx": {
				Name: "xx",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"people": {
				Name: "people",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
			"xxtt1": {
				Name: "xxtt1",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "w_id",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		LowerBound: []interface{}{int64(1)},

		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		LowerBound: []interface{}{int64(11)},

		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: `INSERT INTO xxtt1 SELECT * FROM xxtt1 a WHERE a.w_id = 20;`,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,

						ColumnTypes: []string{qdb.ColumnTypeInteger},
						LowerBound: []interface{}{
							int64(11),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: `
			INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);
			`,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,

						ColumnTypes: []string{qdb.ColumnTypeInteger},
						LowerBound: []interface{}{
							int64(11),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: `
			INSERT INTO "people" ("first_name","last_name","email","id") VALUES ('John','Smith','',1) RETURNING "id"`,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,

						ColumnTypes: []string{qdb.ColumnTypeInteger},
						LowerBound: []interface{}{
							int64(1),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: `
			INSERT INTO xxtt1 (j, w_id) SELECT a, 20 from unnest(ARRAY[10]) a
			`,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,

						ColumnTypes: []string{qdb.ColumnTypeInteger},
						LowerBound: []interface{}{
							int64(11),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "Insert into xx (i, j, k) values (1, 12, 13), (2, 3, 4)",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh1",
						ID:           "id1",
						Distribution: distribution,

						ColumnTypes: []string{qdb.ColumnTypeInteger},
						LowerBound: []interface{}{
							int64(1),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestJoins(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"sshjt1": {
				Name: "sshjt1",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"xjoin": {
				Name: "xjoin",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
			"yjoin": {
				Name: "yjoin",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		LowerBound:   []interface{}{int64(11)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		LowerBound:   []interface{}{int64(11)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "SELECT * FROM sshjt1 a join sshjt1 b ON TRUE WHERE a.i = 12 AND b.j = a.j;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM sshjt1 join sshjt1 ON TRUE WHERE sshjt1.i = 12 AND sshjt1.j = sshjt1.j;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},
						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where w_idx = 15 ORDER BY id;",
			exp:   routingstate.MultiMatchState{},
			err:   nil,
		},

		// sharding columns, but unparsed
		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where i = 15 ORDER BY id;",
			exp:   routingstate.MultiMatchState{},
			err:   nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		if tt.err != nil {
			assert.Equal(tt.err, err, "query %s", tt.query)
		} else {
			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, tt.query)
		}
	}
}

func TestUnnest(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"xxtt1": {
				Name: "xxtt1",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		LowerBound:   []interface{}{int64(11)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		LowerBound:   []interface{}{int64(11)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: "INSERT INTO xxtt1 (j, i) SELECT a, 20 from unnest(ARRAY[10]) a;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "UPDATE xxtt1 set i=a.i, j=a.j from unnest(ARRAY[(1,10)]) as a(i int, j int) where i=20 and xxtt1.j=a.j;",
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh2",
					},
					Matchedkr: &kr.KeyRange{
						ShardID:      "sh2",
						ID:           "id2",
						Distribution: distribution,
						LowerBound: []interface{}{
							int64(11),
						},

						ColumnTypes: []string{qdb.ColumnTypeInteger},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCopySingleShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   routingstate.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"xx": {
				Name: "xx",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "i",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "id1",
		LowerBound:   []interface{}{int64(1)},

		ColumnTypes: []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh2",
		Distribution: distribution,
		ID:           "id2",
		LowerBound:   []interface{}{int64(11)},

		ColumnTypes: []string{qdb.ColumnTypeInteger},
	}).ToDB())

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "COPY xx FROM STDIN WHERE i = 1;",
			exp:   routingstate.MultiMatchState{},
			err:   nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestSetStmt(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query        string
		distribution string
		exp          routingstate.RoutingState
		err          error
	}
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution1 := "ds1"
	distribution2 := "ds2"

	assert.NoError(db.CreateDistribution(context.TODO(), qdb.NewDistribution(distribution1, nil)))
	assert.NoError(db.CreateDistribution(context.TODO(), qdb.NewDistribution(distribution2, nil)))

	err := db.CreateKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:        "sh1",
		DistributionId: distribution1,
		KeyRangeID:     "id1",
		LowerBound:     [][]byte{[]byte("1")},
	})

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:        "sh2",
		DistributionId: distribution2,
		KeyRangeID:     "id2",
		LowerBound:     [][]byte{[]byte("1")},
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "SET extra_float_digits = 3",
			distribution: distribution1,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SET application_name = 'jiofewjijiojioji';",
			distribution: distribution2,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SHOW TRANSACTION ISOLATION LEVEL;",
			distribution: distribution1,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(tt.distribution))

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestRouteWithRules_Select(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query        string
		distribution string
		exp          routingstate.RoutingState
		err          error
	}
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := &qdb.Distribution{
		ID:       "ds1",
		ColTypes: []string{qdb.ColumnTypeVarchar},
		Relations: map[string]*qdb.DistributedRelation{
			"users": {
				Name: "users",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
		},
	}
	unusedDistribution := &qdb.Distribution{
		ID:       "ds2",
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"documents": {
				Name: "documents",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
		},
	}

	assert.NoError(db.CreateDistribution(context.TODO(), distribution))
	assert.NoError(db.CreateDistribution(context.TODO(), unusedDistribution))

	err := db.CreateKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:        "sh1",
		DistributionId: distribution.ID,
		KeyRangeID:     "id1",
		LowerBound:     [][]byte{[]byte("00000000-0000-0000-0000-000000000000")},
	})

	assert.NoError(err)

	err = db.CreateKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:        "sh2",
		DistributionId: unusedDistribution.ID,
		KeyRangeID:     "id2",
		LowerBound:     [][]byte{[]byte("1")},
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "SELECT * FROM information_schema.columns;",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},

		{
			query:        "SELECT * FROM information_schema.sequences;",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM information_schema.columns JOIN tt ON true",
			distribution: distribution.ID,
			exp:          nil,
			err:          qrouter.InformationSchemaCombinedQuery,
		},
		{
			query:        "SELECT * FROM information_schema.columns JOIN pg_class ON true;",
			distribution: distribution.ID,
			exp:          nil,
			err:          qrouter.InformationSchemaCombinedQuery,
		},
		{
			query:        "SELECT * FROM pg_class JOIN users ON true;",
			distribution: distribution.ID,
			exp:          routingstate.MultiMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM pg_tables WHERE schemaname = 'information_schema'",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT current_schema;",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT current_schema();",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT pg_is_in_recovery();",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT set_config('log_statement_stats', 'off', false);",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT 1;",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT true;",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT 'Hello, world!'",
			distribution: distribution.ID,
			exp:          routingstate.RandomMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM users;",
			distribution: distribution.ID,
			exp:          routingstate.MultiMatchState{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM users WHERE id = '5f57cd31-806f-4789-a6fa-1d959ec4c64a';",
			distribution: distribution.ID,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ID:           "id1",
						ShardID:      "sh1",
						Distribution: distribution.ID,
						LowerBound:   []interface{}{"00000000-0000-0000-0000-000000000000"},
						ColumnTypes:  []string{qdb.ColumnTypeVarchar},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		// TODO rewrite routeByClause to support this
		// {
		// 	query:        "SELECT * FROM users WHERE '5f57cd31-806f-4789-a6fa-1d959ec4c64a' = id;",
		// 	distribution: distribution.ID,
		// 	exp: routingstate.ShardMatchState{
		// 		Route: &routingstate.DataShardRoute{
		// 			Shkey: kr.ShardKey{
		// 				Name: "sh1",
		// 			},
		// 			Matchedkr: &kr.KeyRange{
		// 				ID:           "id1",
		// 				ShardID:      "sh1",
		// 				Distribution: distribution.ID,
		// 				LowerBound:   []interface{}{"00000000-0000-0000-0000-000000000000"},
		// 				ColumnTypes:  []string{qdb.ColumnTypeVarchar},
		// 			},
		// 		},
		// 		TargetSessionAttrs: "any",
		// 	},
		// 	err: nil,
		// },
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(tt.distribution))
		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
			assert.Equal(tt.exp, tmp, tt.query)
		} else {
			assert.Error(tt.err, err, tt.query)
		}
	}
}

func TestHashRouting(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query        string
		distribution string
		exp          routingstate.RoutingState
		err          error
	}
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution1 := "ds1"

	assert.NoError(db.CreateDistribution(context.TODO(),
		qdb.NewDistribution(distribution1,
			[]string{qdb.ColumnTypeVarcharHashed})))

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution1,
		ID:           "id1",
		LowerBound: kr.KeyRangeBound{
			uint64(1),
		},
		ColumnTypes: []string{
			qdb.ColumnTypeVarcharHashed,
		},
	}).ToDB(),
	)

	assert.NoError(err)

	err = db.AlterDistributionAttach(context.TODO(), distribution1, []*qdb.DistributedRelation{
		{
			Name: "xx",
			DistributionKey: []qdb.DistributionKeyEntry{
				{
					Column:       "col1",
					HashFunction: "murmur",
				},
			},
		},
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "INSERT INTO xx (col1) VALUES ('Hello, world!');",
			distribution: distribution1,
			exp: routingstate.ShardMatchState{
				Route: &routingstate.DataShardRoute{
					Shkey: kr.ShardKey{
						Name: "sh1",
					},
					Matchedkr: &kr.KeyRange{
						ID: "id1",
						LowerBound: kr.KeyRangeBound{
							uint64(1),
						},
						ShardID:      "sh1",
						Distribution: "ds1",
						ColumnTypes: []string{
							qdb.ColumnTypeVarcharHashed,
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes, session.NewDummyHandler(tt.distribution))
		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, tt.query)
		} else {
			assert.Error(tt.err, err, tt.query)
		}
	}
}
