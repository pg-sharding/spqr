package qrouter_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord/local"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/qrouter"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/lyx/lyx"
)

const MemQDBPath = "memqdb.json"

func TestMultiShardRouting(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "create table xx (i int);",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
		{
			query: " DROP TABLE copy_test;",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
		{
			query: "select 42;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: "alter table xx  add column i int;",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
		{
			query: "vacuum xx;",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
		{
			query: "analyze xx;",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
		{
			query: "cluster xx;",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestComment(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("25"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "select /* oiwejow--23**/ * from  xx where i = 4;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
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

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestSingleShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("25"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "select * from  xx where i = 4;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "INSERT INTO xx (i) SELECT 20;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("25"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxtt1 a WHERE a.i = 21 and w_idj + w_idi != 0;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("25"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},
		{
			query: "select * from  xx where i = 11;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("25"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "Insert into xx (i) values (1), (2)",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "Insert into xx (i) select * from yy where i = 8",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xxmixed WHERE i BETWEEN 22 AND 30 ORDER BY id;;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("25"),
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

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestInsertOffsets(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("21"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: "Insert into xx (i, j, k) values (1, 12, 13), (2, 3, 4)",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
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

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestJoins(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("21"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "SELECT * FROM sshjt1 a join sshjt1 b ON TRUE WHERE a.i = 12 AND b.j = a.j;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("21"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where w_idx = 15 ORDER BY id;'",
			exp:   qrouter.MultiMatchState{},
			err:   nil,
		},

		// sharding columns, but unparsed
		{
			query: "SELECT * FROM xjoin JOIN yjoin on id=w_id where i = 15 ORDER BY id;'",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("21"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: qrouter.ComplexQuery,
		},
	} {
		parserRes, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		tmp, err := pr.Route(context.TODO(), parserRes)

		if tt.err != nil {
			assert.Equal(tt.err, err, "query %s", tt.query)
		} else {
			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp)
		}
	}
}

func TestUnnest(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("21"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{

		{
			query: "INSERT INTO xxtt1 (j, i) SELECT a, 20 from unnest(ARRAY[10]) a;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("21"),
						},
					},
				},
				TargetSessionAttrs: "any",
			},
			err: nil,
		},

		{
			query: "UPDATE xxtt1 set i=a.i, j=a.j from unnest(ARRAY[(1,10)]) as a(i int, j int) where i=20 and xxtt1.j=a.j;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh2",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh2",
							ID:         "id2",
							LowerBound: []byte("11"),
							UpperBound: []byte("21"),
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

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}

func TestCopySingleShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   qrouter.RoutingState
		err   error
	}
	/* TODO: fix by adding configurable setting */
	db, _ := qdb.NewMemQDB(MemQDBPath)

	_ = db.AddShardingRule(context.TODO(), &qdb.ShardingRule{
		ID:        "id1",
		TableName: "",
		Entries: []qdb.ShardingRuleEntry{
			{
				Column: "i",
			},
		},
	})

	err := db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh1",
		KeyRangeID: "id1",
		LowerBound: []byte("1"),
		UpperBound: []byte("11"),
	})

	assert.NoError(err)

	err = db.AddKeyRange(context.TODO(), &qdb.KeyRange{
		ShardID:    "sh2",
		KeyRangeID: "id2",
		LowerBound: []byte("11"),
		UpperBound: []byte("21"),
	})

	assert.NoError(err)

	lc := local.NewLocalCoordinator(db)

	pr, err := qrouter.NewProxyRouter(map[string]*config.Shard{
		"sh1": {
			Hosts: nil,
		},
		"sh2": {
			Hosts: nil,
		},
	}, lc, &config.QRouter{
		DefaultRouteBehaviour: "BLOCK",
	})

	assert.NoError(err)

	for _, tt := range []tcase{
		{
			query: "COPY xx FROM STDIN WHERE i = 1;",
			exp: qrouter.ShardMatchState{
				Routes: []*qrouter.DataShardRoute{
					{
						Shkey: kr.ShardKey{
							Name: "sh1",
						},
						Matchedkr: &kr.KeyRange{
							ShardID:    "sh1",
							ID:         "id1",
							LowerBound: []byte("1"),
							UpperBound: []byte("11"),
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

		tmp, err := pr.Route(context.TODO(), parserRes)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp)
	}
}
