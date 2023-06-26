package rule

import (
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/route"
)

type MatchMgr[T any] interface {
	MatchKey(key route.Key) (*T, error)
	UnderlyingEntityName() string
}

type RulesMgr interface {
	MatchKeyFrontend(key route.Key) (*config.FrontendRule, error)
	MatchKeyBackend(key route.Key) (*config.BackendRule, error)

	Reload(frmp map[route.Key]*config.FrontendRule,
		bemp map[route.Key]*config.BackendRule,
		dfr *config.FrontendRule,
		dbe *config.BackendRule)
}

type RulesMgrImpl struct {
	mu sync.Mutex
	fe MatchMgr[config.FrontendRule]
	be MatchMgr[config.BackendRule]
}

func (F *RulesMgrImpl) Reload(frmp map[route.Key]*config.FrontendRule, bemp map[route.Key]*config.BackendRule, dfr *config.FrontendRule, dbe *config.BackendRule) {
	F.mu.Lock()
	defer F.mu.Unlock()

	fe := &MgrImpl[config.FrontendRule]{
		rule: frmp,
		defaultRuleAllocator: func(key route.Key) *config.FrontendRule {
			if dfr == nil {
				return nil
			}
			return &config.FrontendRule{
				Usr:                   key.Usr(),
				DB:                    key.DB(),
				AuthRule:              dfr.AuthRule,
				PoolMode:              dfr.PoolMode,
				PoolPreparedStatement: dfr.PoolPreparedStatement,
			}
		},
	}

	be := &MgrImpl[config.BackendRule]{
		rule: bemp,
		defaultRuleAllocator: func(key route.Key) *config.BackendRule {
			if dbe == nil {
				return nil
			}
			return &config.BackendRule{
				Usr:             key.Usr(),
				DB:              key.DB(),
				AuthRule:        dbe.AuthRule,
				ConnectionLimit: dbe.ConnectionLimit,
			}
		},
	}

	F.fe = fe
	F.be = be
}

func (F *RulesMgrImpl) MatchKeyFrontend(key route.Key) (*config.FrontendRule, error) {
	return F.fe.MatchKey(key)
}

func (F *RulesMgrImpl) MatchKeyBackend(key route.Key) (*config.BackendRule, error) {
	return F.be.MatchKey(key)
}

type MgrImpl[T any] struct {
	rule map[route.Key]*T

	defaultRuleAllocator func(key route.Key) *T

	underlyingEntityName string
}

func (r *MgrImpl[T]) UnderlyingEntityName() string {
	return r.underlyingEntityName
}

func (m *MgrImpl[T]) MatchKey(key route.Key) (*T, error) {
	matchRule, ok := m.rule[key]
	if ok {
		return matchRule, nil
	}

	matchRule = m.defaultRuleAllocator(key)
	// may return null
	if matchRule != nil {
		// ok
		// created dynamic rule
		return matchRule, nil
	}

	return nil, fmt.Errorf("failed to route frontend for client:"+
		" route for user:%s and db:%s is unconfigured in %s", key.Usr(), key.DB(), m.UnderlyingEntityName())
}

func NewMgr(frmp map[route.Key]*config.FrontendRule,
	bemp map[route.Key]*config.BackendRule,
	dfr *config.FrontendRule,
	dbe *config.BackendRule) RulesMgr {
	fe := &MgrImpl[config.FrontendRule]{
		rule: frmp,
		defaultRuleAllocator: func(key route.Key) *config.FrontendRule {
			if dfr == nil {
				return nil
			}
			return &config.FrontendRule{
				Usr:                   key.Usr(),
				DB:                    key.DB(),
				AuthRule:              dfr.AuthRule,
				PoolMode:              dfr.PoolMode,
				PoolPreparedStatement: dfr.PoolPreparedStatement,
			}
		},
		underlyingEntityName: "frontend rules",
	}

	be := &MgrImpl[config.BackendRule]{
		rule: bemp,
		defaultRuleAllocator: func(key route.Key) *config.BackendRule {
			if dbe == nil {
				return nil
			}
			return &config.BackendRule{
				Usr:             key.Usr(),
				DB:              key.DB(),
				AuthRule:        dbe.AuthRule,
				ConnectionLimit: dbe.ConnectionLimit,
			}
		},
		underlyingEntityName: "backend rules",
	}

	ret := &RulesMgrImpl{
		fe: fe,
		be: be,
	}
	return ret
}

var _ MatchMgr[interface{}] = &MgrImpl[interface{}]{}
