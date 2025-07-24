package qrouter_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rmeta"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/lyx/lyx"
)

const MemQDBPath = "memqdb.json"

func TestMultiShardRouting(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "create table xx (i int);",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "DROP TABLE copy_test;",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "select current_schema;",
			exp:   &plan.RandomDispatchPlan{},
			err:   nil,
		},
		{
			query: "select current_schema();",
			exp:   &plan.RandomDispatchPlan{},
			err:   nil,
		},
		{
			query: "alter table xx  add column i int;",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "vacuum xx;",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "analyze xx;",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "cluster xx;",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},
			err: nil,
		},
		{
			query: "GRANT SELECT ON TABLE odssd.'eee' TO pp2;			",
			exp: &plan.ScatterPlan{
				IsDDL: true,
			},

			err: nil,
		},
		{
			query: "SELECT * FROM pg_catalog.pg_type",
			exp:   &plan.RandomDispatchPlan{},
			err:   nil,
		},

		{
			query: "SELECT * FROM pg_class",
			exp:   &plan.RandomDispatchPlan{},
			err:   nil,
		},
		{
			query: `SELECT count(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA() AND table_name = 'people' AND table_type = 'BASE TABLE'`,
			exp:   &plan.RandomDispatchPlan{},
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestScatterQueryRoutingEngineV2(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "ds1"
	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"distrr_mm_test": {
				Name: "distrr_mm_test",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "UPDATE distrr_mm_test SET t = 'm' WHERE id IN (3, 34) /* __spqr__engine_v2: true */;",
			exp: &plan.ScatterPlan{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{},
				},
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
			err: nil,
		},
		{
			query: "DELETE FROM distrr_mm_test WHERE id IN (3, 34) /* __spqr__engine_v2: true */;",
			exp: &plan.ScatterPlan{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{},
				},
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
			err: nil,
		},
		{
			query: "INSERT INTO distrr_mm_test VALUES (3), (34) /* __spqr__engine_v2: true */;",
			exp:   nil,
			err:   rerrors.ErrComplexQuery,
		},
		{
			query: "INSERT INTO distrr_mm_test (id) VALUES (3), (34) /* __spqr__engine_v2: true */;",
			exp:   nil,
			err:   rerrors.ErrComplexQuery,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		dh.SetEnhancedMultiShardProcessing(false, true)

		tmp, err := pr.PlanQuery(context.TODO(), parserRes, dh)

		if tt.err != nil {
			assert.Equal(tt.err, err, tt.query)
		} else {
			tmp.SetStmt(nil) /* dont check stmt */

			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, tt.query)
		}
	}
}

func TestReferenceRelationRouting(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)
	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distributions.REPLICATED,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"test_ref_rel": {
				Name: "test_ref_rel",
			},
		},
	})

	_ = db.CreateReferenceRelation(context.TODO(), &qdb.ReferenceRelation{
		TableName: "test_ref_rel",
	})

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: `INSERT INTO test_ref_rel VALUES(1) returning *;`,
			exp: &plan.DataRowFilter{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ScatterPlan{
						SubPlan: &plan.ModifyTable{},
					},
					ExecTargets: []kr.ShardKey{
						{
							Name: "sh1",
						},
						{
							Name: "sh2",
						},
					},
				},
			},
		},
		{
			query: `INSERT INTO test_ref_rel VALUES(1) ;`,
			exp: &plan.ScatterPlan{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{},
				},
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
		},
		{
			query: `UPDATE test_ref_rel SET i = i + 1 ;`,
			exp: &plan.ScatterPlan{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{},
				},
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
		},
		{
			query: `DELETE FROM test_ref_rel WHERE i = 2;`,
			exp: &plan.ScatterPlan{
				SubPlan: &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{},
				},
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)
		dh := session.NewDummyHandler("dd")
		dh.SetEnhancedMultiShardProcessing(false, true)
		pr.SetQuery(&tt.query)

		tmp, err := pr.PlanQuery(context.TODO(), parserRes, dh)
		if tt.err == nil {

			tmp.SetStmt(nil) /* dont check stmt */

			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, tt.query)
		} else {

			assert.Equal(err, tt.err, tt.query)
		}
	}
}

func TestComment(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "select /* oiwejow--23**/ * from  xx where i = 4;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCTE(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: `
			WITH vals (y, z, x) AS (
				VALUES (
					2,
					4,
					1
				)
			)
			SELECT 
				*
			FROM t r
			JOIN vals 
				ON r.i = vals.x;
			`,
			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},

		{
			query: `
			WITH vals (x, y, z) AS (
				VALUES (
					1,
					2,
					4
				)
			)
			SELECT 
				*
			FROM t r
			JOIN vals 
				ON r.i = vals.x;
			`,
			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},

		{
			query: `
			WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM t t, vv v  WHERE t.i = v.x;`,
			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},
		{
			query: `
			WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM t t, vv  WHERE t.i = vv.x;`,
			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},

		{
			query: `
			WITH vv AS 
				(SELECT i + 1 FROM t WHERE i = 11)
			INSERT INTO t (i) TABLE vv;
			`,

			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},

		{
			query: `WITH qqq AS (
				
			  DELETE FROM t
			  WHERE i = 10 and (k, j) IN (
			  (12::int, 14))
			  )

			  SELECT * FROM qqq;
			`,
			err: nil,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
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
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
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
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
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
		// 	exp: plan.SkipRoutingState{},
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
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

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
		exp   plan.Plan
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
			"tt": {
				Name: "tt",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
			"tt2": {
				Name: "tt2",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{

		/* TODO: fix */
		// /* should not be routed to one shard */
		// {
		// 	query: "SELECT * FROM xxtt1 a WHERE i IN (1,11,111)",
		// 	exp:   plan.MultiMatchState{},
		// 	err:   nil,
		// },

		// {
		// 	query: `INSERT INTO t (i, b, c) SELECT 1,2,3 UNION ALL SELECT 2, 3, 4;`,
		// 	exp:   plan.ShardDispatchPlan{},
		// 	err:   nil,
		// },
		{
			query: ` select * from tt where id in (select * from tt2 g where g.id = 7);`,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
		{
			query: "SELECT * FROM sh1.xxtt1 WHERE sh1.xxtt1.i = 21;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxtt1 a WHERE a.i = 21 and w_idj + w_idi != 0;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxtt1 a WHERE a.i = '21' and w_idj + w_idi != 0;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
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
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
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
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
		},
		{
			query: "select * from  xx where i = 4;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "INSERT INTO xx (i) SELECT 20;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "select * from  xx where i = 11;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "Insert into xx (i) values (1), (2)",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		/* TODO: same query but without alias should work:
		* Insert into xx (i) select * from yy where i = 8
		 */
		{
			query: "Insert into xx (i) select * from yy a where a.i = 8",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxmixed WHERE i BETWEEN 22 AND 30 ORDER BY id;;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM t WHERE i = 12 AND j = 1;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		/* TODO: fix aliasing here  */
		{
			query: "SELECT * FROM t t WHERE t.i = 12 UNION ALL SELECT * FROM xxmixed x WHERE x.i = 22;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestInsertOffsets(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: `INSERT INTO xxtt1 SELECT * FROM xxtt1 a WHERE a.w_id = 20;`,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: `
			INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);
			`,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
		{
			query: `
			INSERT INTO "people" ("first_name","last_name","email","id") VALUES ('John','Smith','',1) RETURNING "id"`,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
		{
			query: `
			INSERT INTO xxtt1 (j, w_id) SELECT a, 20 from unnest(ARRAY[10]) a
			`,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "Insert into xx (i, j, k) values (1, 12, 13), (2, 3, 4)",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestJoins(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "SELECT * FROM sshjt1 a join sshjt1 b ON TRUE WHERE a.i = 12 AND b.j = a.j;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM sshjt1 join sshjt1 ON TRUE WHERE sshjt1.i = 12 AND sshjt1.j = sshjt1.j;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where w_idx = 15 ORDER BY id;",
			exp: &plan.ScatterPlan{
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
			err: nil,
		},

		// sharding columns, but unparsed
		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where i = 15 ORDER BY id;",
			exp: &plan.ScatterPlan{ExecTargets: []kr.ShardKey{
				{
					Name: "sh1",
				},
				{
					Name: "sh2",
				},
			},
			},
			err: nil,
		},

		// non-sharding columns
		{
			query: "SELECT * FROM xjoin a JOIN yjoin b ON a.j = b.j;",
			exp: &plan.ScatterPlan{
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)

		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

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
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: "INSERT INTO xxtt1 (j, i) SELECT a, 20 from unnest(ARRAY[10]) a;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: "UPDATE xxtt1 set i=a.i, j=a.j from unnest(ARRAY[(1,10)]) as a(i int, j int) where i=20 and xxtt1.j=a.j;",
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh2",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCopySingleShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "COPY xx FROM STDIN WHERE i = 1;",
			exp:   &plan.CopyPlan{},
			err:   nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		dh.SetDefaultRouteBehaviour(false, "BLOCK")

		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())

		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCopyMultiShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "COPY xx FROM STDIN",
			exp:   &plan.CopyPlan{},
			err:   nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(distribution)
		dh.SetDefaultRouteBehaviour(false, "BLOCK")
		dh.SetScatterQuery(false)

		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestSetStmt(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query        string
		distribution string
		exp          plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "SET extra_float_digits = 3",
			distribution: distribution1,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SET application_name = 'jiofewjijiojioji';",
			distribution: distribution2,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SHOW TRANSACTION ISOLATION LEVEL;",
			distribution: distribution1,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(tt.distribution)

		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, tt.query)
	}
}

func TestRouteWithRules_Select(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query        string
		distribution string
		exp          plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "SELECT * FROM pg_class a JOIN users b ON true WHERE b.id = '00000000-0000-1111-0000-000000000000';",
			distribution: distribution.ID,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query:        "SELECT NOT pg_is_in_recovery();",
			distribution: distribution.ID,
			exp: &plan.VirtualPlan{
				VirtualRowCols: []pgproto3.FieldDescription{
					{
						Name:         []byte("pg_is_in_recovery"),
						DataTypeOID:  catalog.ARRAYOID,
						TypeModifier: -1,
						DataTypeSize: 1,
					},
				},
				VirtualRowVals: [][]byte{[]byte{byte('t')}},
			},
			err: nil,
		},
		{
			query:        "SELECT * FROM information_schema.columns;",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},

		{
			query:        "SELECT * FROM information_schema.sequences;",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM information_schema.columns JOIN tt ON true",
			distribution: distribution.ID,
			exp:          nil,
			err:          rerrors.ErrInformationSchemaCombinedQuery,
		},
		{
			query:        "SELECT * FROM information_schema.columns JOIN pg_class ON true;",
			distribution: distribution.ID,
			exp:          nil,
			err:          rerrors.ErrInformationSchemaCombinedQuery,
		},
		{
			query:        "SELECT * FROM pg_class JOIN users ON true;",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT * FROM pg_tables WHERE schemaname = 'information_schema'",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT current_schema;",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT current_schema();",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT pg_is_in_recovery();",
			distribution: distribution.ID,
			exp: &plan.VirtualPlan{
				VirtualRowCols: []pgproto3.FieldDescription{
					{
						Name:         []byte("pg_is_in_recovery"),
						DataTypeOID:  catalog.ARRAYOID,
						TypeModifier: -1,
						DataTypeSize: 1,
					},
				},
				VirtualRowVals: [][]byte{{byte('f')}},
			},
			err: nil,
		},
		{
			query:        "SELECT set_config('log_statement_stats', 'off', false);",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT version()",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT 1;",
			distribution: distribution.ID,
			exp: &plan.VirtualPlan{
				VirtualRowCols: []pgproto3.FieldDescription{
					{
						Name:         []byte("?column?"),
						DataTypeOID:  catalog.INT4OID,
						TypeModifier: -1,
						DataTypeSize: 4,
					},
				},
				VirtualRowVals: [][]byte{[]byte("1")},
			},
			err: nil,
		},
		{
			query:        "SELECT true;",
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},
		{
			query:        "SELECT 'Hello, world!'",
			distribution: distribution.ID,
			exp: &plan.VirtualPlan{
				VirtualRowCols: []pgproto3.FieldDescription{
					pgproto3.FieldDescription{
						Name:         []byte("?column?"),
						DataTypeOID:  catalog.TEXTOID,
						TypeModifier: -1,
						DataTypeSize: -1,
					},
				},
				VirtualRowVals: [][]byte{[]byte("Hello, world!")},
			},
			err: nil,
		},
		{
			query:        "SELECT * FROM users;",
			distribution: distribution.ID,
			exp: &plan.ScatterPlan{
				ExecTargets: []kr.ShardKey{
					{
						Name: "sh1",
					},
					{
						Name: "sh2",
					},
				},
			},
			err: nil,
		},
		{
			query:        "SELECT * FROM users WHERE id = '5f57cd31-806f-4789-a6fa-1d959ec4c64a';",
			distribution: distribution.ID,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},

		{
			query: `
			SELECT c.relname, NULL::pg_catalog.text FROM pg_catalog.pg_class c WHERE c.relkind IN ('r', 'p') AND (c.relname) LIKE 'x%' AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')
UNION ALL
SELECT NULL::pg_catalog.text, n.nspname FROM pg_catalog.pg_namespace n WHERE n.nspname LIKE 'x%' AND n.nspname NOT LIKE E'pg\\_%'
LIMIT 1000
`,
			distribution: distribution.ID,
			exp:          &plan.RandomDispatchPlan{},
			err:          nil,
		},

		// TODO rewrite routeByClause to support this
		// {
		// 	query:        "SELECT * FROM users WHERE '5f57cd31-806f-4789-a6fa-1d959ec4c64a' = id;",
		// 	distribution: distribution.ID,
		// 	exp: plan.ShardDispatchPlan{
		// 		Route: &plan.DataShardRoute{
		// 			Shkey: kr.ShardKey{
		// 				Name: "sh1",
		// 			},
		// 			MatchedKr: &kr.KeyRange{
		// 				ID:           "id1",
		// 				ShardID:      "sh1",
		// 				Distribution: distribution.ID,
		// 				LowerBound:   []interface{}{"00000000-0000-0000-0000-000000000000"},
		// 				ColumnTypes:  []string{qdb.ColumnTypeVarchar},
		// 			},
		// 		},
		//
		//		TargetSessionAttrs: config.TargetSessionAttrsRW,
		// 	},
		// 	err: nil,
		// },
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(tt.distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

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
		exp          plan.Plan
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

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	}, nil)

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query:        "INSERT INTO xx (col1) VALUES ('Hello, world!');",
			distribution: distribution1,
			exp: &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: "sh1",
				},
				TargetSessionAttrs: config.TargetSessionAttrsRW,
			},
			err: nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		dh := session.NewDummyHandler(tt.distribution)
		rm := rmeta.NewRoutingMetadataContext(dh, pr.Mgr())
		tmp, _, err := pr.RouteWithRules(context.TODO(), rm, parserRes, dh.GetTsa())

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, tt.query)
		} else {
			assert.Error(tt.err, err, tt.query)
		}
	}
}
func prepareTestCheckTableIsRoutable(t *testing.T) (*qrouter.ProxyQrouter, error) {
	db, _ := qdb.NewMemQDB(MemQDBPath)
	distribution := "dd"

	_ = db.CreateDistribution(context.TODO(), &qdb.Distribution{
		ID:       distribution,
		ColTypes: []string{qdb.ColumnTypeInteger},
		Relations: map[string]*qdb.DistributedRelation{
			"table1": {
				Name: "table1",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
			"table2": {
				Name:       "table2",
				SchemaName: "schema2",
				DistributionKey: []qdb.DistributionKeyEntry{
					{
						Column: "id",
					},
				},
			},
		},
	})

	err := db.CreateKeyRange(context.TODO(), (&kr.KeyRange{
		ShardID:      "sh1",
		Distribution: distribution,
		ID:           "krid1",
		LowerBound:   []interface{}{int64(11)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}).ToDB())
	if err != nil {
		return nil, err
	}

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)

	router, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {},
		"sh2": {},
	}, lc, &config.QRouter{}, nil)

	return router, err
}

func TestCheckTableIsRoutable(t *testing.T) {
	type tcase struct {
		query string
		err   error
	}
	assert := assert.New(t)
	router, err := prepareTestCheckTableIsRoutable(t)
	assert.NoError(err)

	ctx := context.Background()
	for nn, tt := range []tcase{
		{
			query: "create table table1 (id int)",
			err:   nil,
		},
		{
			query: "create table table1 (id1 int)",
			err:   fmt.Errorf("create table stmt ignored: no sharding rule columns found"),
		},
		{
			query: "create table schema2.table2 (id int, dat varchar)",
			err:   nil,
		},
		{
			query: "create table schema2.table2Err (id int, dat varchar)",
			err:   fmt.Errorf("distribution for relation \"schema2.table2Err\" not found"),
		},
	} {
		stmt, err := lyx.Parse(tt.query)
		assert.NoError(err)
		switch node := stmt.(type) {
		case *lyx.CreateTable:
			actualErr := router.CheckTableIsRoutable(ctx, node)
			if tt.err == nil {
				assert.NoError(actualErr, "case #%d", nn)
			} else {
				assert.Error(actualErr, "case #%d", nn)
			}
		default:
			assert.NoError(fmt.Errorf("no create statement"))
		}
	}

}
