package rulemgr

import (
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/route"
)

type MatchMgr[T any] interface {
	MatchKey(key route.Key, underlyingEntityName string) (*T, error)
}

type RulesMgr interface {
	MatchKeyFrontend(key route.Key) (*config.FrontendRule, error)
	MatchKeyBackend(key route.Key) (*config.BackendRule, error)
	Reload(frules []*config.FrontendRule, brules []*config.BackendRule)
}

type RulesMgrImpl struct {
	mu sync.Mutex
	fe MatchMgr[config.FrontendRule]
	be MatchMgr[config.BackendRule]
}

// TODO : unit tests
func (F *RulesMgrImpl) Reload(frules []*config.FrontendRule, brules []*config.BackendRule) {
	mapFR, mapBE, defaultFR, defaultBE := parseRules(frules, brules)
	F.mu.Lock()
	defer F.mu.Unlock()

	fe := &MgrImpl[config.FrontendRule]{
		rule: mapFR,
		defaultRuleAllocator: func(key route.Key) *config.FrontendRule {
			if defaultFR == nil {
				return nil
			}
			spqrlog.Zero.Debug().
				Str("db", defaultFR.DB).
				Str("user", defaultFR.Usr).
				Msg("generating new dynamic rule")
			return &config.FrontendRule{
				DB:                    key.DB(),
				Usr:                   key.Usr(),
				SearchPath:            defaultFR.SearchPath,
				AuthRule:              defaultFR.AuthRule,
				PoolMode:              defaultFR.PoolMode,
				PoolDiscard:           defaultFR.PoolDiscard,
				PoolRollback:          defaultFR.PoolRollback,
				PoolPreparedStatement: defaultFR.PoolPreparedStatement,
				PoolDefault:           defaultFR.PoolDefault,
			}
		},
	}

	be := &MgrImpl[config.BackendRule]{
		rule: mapBE,
		defaultRuleAllocator: func(key route.Key) *config.BackendRule {
			if defaultBE == nil {
				return nil
			}
			return &config.BackendRule{
				DB:                key.DB(),
				Usr:               key.Usr(),
				AuthRules:         defaultBE.AuthRules,
				DefaultAuthRule:   defaultBE.DefaultAuthRule,
				PoolDefault:       defaultBE.PoolDefault,
				ConnectionLimit:   defaultBE.ConnectionLimit,
				ConnectionRetries: defaultBE.ConnectionRetries,
				ConnectionTimeout: defaultBE.ConnectionTimeout,
				KeepAlive:         defaultBE.KeepAlive,
				TcpUserTimeout:    defaultBE.TcpUserTimeout,
			}
		},
	}

	F.fe = fe
	F.be = be
}

// TODO : unit tests
func (F *RulesMgrImpl) MatchKeyFrontend(key route.Key) (*config.FrontendRule, error) {
	return F.fe.MatchKey(key, "frontend rules")
}

// TODO : unit tests
func (F *RulesMgrImpl) MatchKeyBackend(key route.Key) (*config.BackendRule, error) {
	return F.be.MatchKey(key, "backend rules")
}

type MgrImpl[T any] struct {
	rule                 map[route.Key]*T
	defaultRuleAllocator func(key route.Key) *T
}

// TODO : unit tests
func (m *MgrImpl[T]) MatchKey(key route.Key, underlyingEntityName string) (*T, error) {
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
		" route for user:%s and db:%s is unconfigured in %s", key.Usr(), key.DB(), underlyingEntityName)
}

func parseRules(cfgFrontendRules []*config.FrontendRule, cfgBackendRules []*config.BackendRule) (map[route.Key]*config.FrontendRule, map[route.Key]*config.BackendRule, *config.FrontendRule, *config.BackendRule) {
	frontendRules := map[route.Key]*config.FrontendRule{}
	var defaultFrontendRule *config.FrontendRule
	for _, frontendRule := range cfgFrontendRules {
		if frontendRule.PoolDefault {
			defaultFrontendRule = frontendRule
			continue
		}
		spqrlog.Zero.Debug().
			Str("db", frontendRule.DB).
			Str("user", frontendRule.Usr).
			Msg("adding frontend rule")
		key := *route.NewRouteKey(frontendRule.Usr, frontendRule.DB)
		frontendRules[key] = frontendRule
	}

	backendRules := map[route.Key]*config.BackendRule{}
	var defaultBackendRule *config.BackendRule
	for _, backendRule := range cfgBackendRules {
		if backendRule.PoolDefault {
			defaultBackendRule = backendRule
			continue
		}
		key := *route.NewRouteKey(backendRule.Usr, backendRule.DB)
		backendRules[key] = backendRule
	}

	return frontendRules, backendRules, defaultFrontendRule, defaultBackendRule
}

func NewMgr(frules []*config.FrontendRule, brules []*config.BackendRule) RulesMgr {
	frmp, bemp, dfr, dbr := parseRules(frules, brules)
	return &RulesMgrImpl{
		fe: &MgrImpl[config.FrontendRule]{
			rule: frmp,
			defaultRuleAllocator: func(key route.Key) *config.FrontendRule {
				if dfr == nil {
					return nil
				}
				// TODO add missing fields
				return &config.FrontendRule{
					Usr:                   key.Usr(),
					DB:                    key.DB(),
					AuthRule:              dfr.AuthRule,
					PoolMode:              dfr.PoolMode,
					PoolPreparedStatement: dfr.PoolPreparedStatement,
				}
			},
		},
		be: &MgrImpl[config.BackendRule]{
			rule: bemp,
			defaultRuleAllocator: func(key route.Key) *config.BackendRule {
				if dbr == nil {
					return nil
				}
				// TODO add missing fields
				return &config.BackendRule{
					Usr:               key.Usr(),
					DB:                key.DB(),
					AuthRules:         dbr.AuthRules,
					DefaultAuthRule:   dbr.DefaultAuthRule,
					ConnectionLimit:   dbr.ConnectionLimit,
					ConnectionRetries: dbr.ConnectionRetries,
					ConnectionTimeout: dbr.ConnectionTimeout,
				}
			},
		},
	}
}

var _ MatchMgr[any] = &MgrImpl[any]{}
