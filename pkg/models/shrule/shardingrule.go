package shrule

import (
	"fmt"

	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type ShardingRuleEntry struct {
	Column       string
	HashFunction string
}

func NewShardingRuleEntry(column string, hashFunction string) *ShardingRuleEntry {
	return &ShardingRuleEntry{
		Column:       column,
		HashFunction: hashFunction,
	}
}

type ShardingRule struct {
	Id        string
	TableName string
	entries   []ShardingRuleEntry
	Dataspace string
}

func NewShardingRule(id string, tableName string, entries []ShardingRuleEntry, dataspaceId string) *ShardingRule {
	return &ShardingRule{
		Id:        id,
		TableName: tableName,
		entries:   entries,
		Dataspace: dataspaceId,
	}
}

func (s *ShardingRule) ID() string {
	return s.Id
}

func (s *ShardingRule) Entries() []ShardingRuleEntry {
	return s.entries
}

func (s *ShardingRule) String() string {
	tableName := s.TableName
	if tableName == "" {
		tableName = "*"
	}

	entries := func() []string {
		var ret []string
		for _, el := range s.Entries() {
			ret = append(ret, fmt.Sprintf("%v, hash: x->x", el.Column))
		}
		return ret
	}()

	return fmt.Sprintf("sharding rule %v for table (%v) with columns %+v for dataspace %v", s.Id, tableName, entries, s.Dataspace)
}

func ShardingRuleFromDB(rule *qdb.ShardingRule) *ShardingRule {
	ret := &ShardingRule{
		Id:        rule.ID,
		TableName: rule.TableName,
		Dataspace: rule.DataspaceId,
	}
	for _, el := range rule.Entries {
		ret.entries = append(ret.entries, ShardingRuleEntry{
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleToDB(rule *ShardingRule) *qdb.ShardingRule {
	ret := &qdb.ShardingRule{
		ID:          rule.Id,
		TableName:   rule.TableName,
		DataspaceId: rule.Dataspace,
	}
	for _, el := range rule.entries {
		ret.Entries = append(ret.Entries, qdb.ShardingRuleEntry{
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleToProto(rule *ShardingRule) *proto.ShardingRule {
	ret := &proto.ShardingRule{
		Id:        rule.Id,
		TableName: rule.TableName,
	}
	for _, el := range rule.entries {
		ret.ShardingRuleEntry = append(ret.ShardingRuleEntry, &proto.ShardingRuleEntry{
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleFromProto(rule *proto.ShardingRule) *ShardingRule {
	ret := &ShardingRule{
		Id:        rule.Id,
		TableName: rule.TableName,
	}
	for _, el := range rule.ShardingRuleEntry {
		ret.entries = append(ret.entries, ShardingRuleEntry{
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func (shrule *ShardingRule) Includes(rule *ShardingRule) bool {
	exCols := map[string]struct{}{}
	for _, entry := range shrule.Entries() {
		exCols[entry.Column] = struct{}{}
	}

	for _, entry := range rule.Entries() {
		// our sharding rule does not have this, column
		// so router can distinguish routing rules
		if _, ok := exCols[entry.Column]; !ok {
			return false
		}
	}

	return true
}
