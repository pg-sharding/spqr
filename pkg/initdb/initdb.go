package initdb

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	qlog "github.com/pg-sharding/spqr/qdb/qlog/provider"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func processDropInit(ctx context.Context, dstmt spqrparser.Statement, qdb qdb.QDB) error {
	switch stmt := dstmt.(type) {
	case *spqrparser.KeyRangeSelector:
		if stmt.KeyRangeID == "*" {
			return qdb.DropKeyRangeAll(ctx)
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s", stmt.KeyRangeID)
			return qdb.DropKeyRange(ctx, stmt.KeyRangeID)
		}
	case *spqrparser.ShardingRuleSelector:
		if stmt.ID == "*" {
			if rules, err := qdb.DropShardingRuleAll(ctx); err != nil {
				return err
			} else {
				return qdb.DropShardingRule(ctx, func() string {
					var ret []string

					for _, rule := range rules {
						ret = append(ret, rule.ID)
					}

					return strings.Join(ret, ",")
				}())
			}
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s", stmt.ID)
			return qdb.DropShardingRule(ctx, stmt.ID)
		}
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

func processCreateInit(ctx context.Context, astmt spqrparser.Statement, qdb qdb.QDB) error {
	switch stmt := astmt.(type) {
	case *spqrparser.DataspaceDefinition:
		/* TODO: fix */
		return nil
	case *spqrparser.ShardingRuleDefinition:
		entries := make([]shrule.ShardingRuleEntry, 0)
		for _, el := range stmt.Entries {
			entries = append(entries, *shrule.NewShardingRuleEntry(el.Column, el.HashFunction))
		}

		rule := shrule.NewShardingRule(stmt.ID, stmt.TableName, entries)

		return qdb.AddShardingRule(ctx, shrule.ShardingRuleToDB(rule))
	case *spqrparser.KeyRangeDefinition:
		return qdb.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt).ToDB())
	default:
		return nil
	}
}

func ProcessQueryInit(qdb qdb.QDB, q string) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		return err
	}

	switch stmt := tstmt.(type) {
	case *spqrparser.Drop:
		return processDropInit(context.TODO(), stmt.Element, qdb)
	case *spqrparser.Create:
		return processCreateInit(context.TODO(), stmt.Element, qdb)
	case *spqrparser.MoveKeyRange:
		/* TODO: fix */

		return nil

	case *spqrparser.RegisterRouter:
		/* TODO: fix */

		return nil
	case *spqrparser.UnregisterRouter:
		/* TODO: fix */

		return nil
		/* TODO: fix */

		return nil
	case *spqrparser.Unlock:
		/* TODO: fix */

		return nil
	case *spqrparser.Show:
		/* should not happen */
		return nil
	default:
		return nil
	}
}

func InitDB(qdb qdb.QDB, wordir string, initfiles []string) error {

	log := qlog.NewLocalQlog(filepath.Join(wordir, "qlog.ql"))

	for _, file := range initfiles {
		tmpq := qlog.NewLocalQlog(file)
		qs, err := tmpq.Recover(context.TODO())
		if err != nil {
			return err
		}
		for _, q := range qs {
			ProcessQueryInit(qdb, q)
		}
	}

	qs, err := log.Recover(context.TODO())
	if err != nil {
		return err
	}

	for _, q := range qs {
		ProcessQueryInit(qdb, q)
	}

	return nil
}
