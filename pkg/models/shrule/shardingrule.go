package shrule

import (
	"github.com/pg-sharding/spqr/qdb"
	proto "github.com/pg-sharding/spqr/router/protos"
)

type ShardingRuleEntry struct {
	TableName    string
	Column       string
	HashFunction string
}

type ShardingRule struct {
	Id      string
	entries []ShardingRuleEntry
}

// local TableName sharding rule -> route to world

func NewShardingRule(id string, entries []ShardingRuleEntry) *ShardingRule {
	return &ShardingRule{
		Id:      id,
		entries: entries,
	}
}

func (s *ShardingRule) ID() string {
	return s.Id
}

func (s *ShardingRule) Entries() []ShardingRuleEntry {
	return s.entries
}

func ShardingRuleFromDB(rule *qdb.ShardingRule) *ShardingRule {
	ret := &ShardingRule{
		Id: rule.Id,
	}
	for _, el := range rule.Entries {
		ret.entries = append(ret.entries, ShardingRuleEntry{
			TableName:    el.TableName,
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleToDB(rule *ShardingRule) *qdb.ShardingRule {
	ret := &qdb.ShardingRule{
		Id: rule.Id,
	}
	for _, el := range rule.entries {
		ret.Entries = append(ret.Entries, qdb.ShardingRuleEntry{
			TableName:    el.TableName,
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleToProto(rule *ShardingRule) *proto.ShardingRule {
	ret := &proto.ShardingRule{
		Id: rule.Id,
	}
	for _, el := range rule.entries {
		ret.ShardingRuleEntry = append(ret.ShardingRuleEntry, &proto.ShardingRuleEntry{
			TableName:    el.TableName,
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}

func ShardingRuleFromProto(rule *proto.ShardingRule) *ShardingRule {
	ret := &ShardingRule{
		Id: rule.Id,
	}
	for _, el := range rule.ShardingRuleEntry {
		ret.entries = append(ret.entries, ShardingRuleEntry{
			TableName:    el.TableName,
			Column:       el.Column,
			HashFunction: el.HashFunction,
		})
	}

	return ret
}
