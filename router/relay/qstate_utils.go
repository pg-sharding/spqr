package relay

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/client"
)

func virtualParamTransformName(name string) string {
	retName := name
	if after, ok := strings.CutPrefix(retName, "__spqr__."); ok {
		retName = "__spqr__" + after
	}

	return retName
}

func DispatchSlice(qd *QueryDesc,
	p plan.Plan, cl client.RouterClient, replyCl bool) error {

	serv := cl.Server()

	var et []kr.ShardKey

	if p == nil {
		for _, ds := range serv.Datashards() {
			et = append(et, ds.SHKey())
		}
	} else {
		et = p.ExecutionTargets()

		if len(et) == 0 {
			for _, ds := range serv.Datashards() {
				et = append(et, ds.SHKey())
			}
		}
	}

	for _, targ := range et {

		if qd.simple {

			/*
			* This is only execution path for non-top level slice
			 */
			if p != nil {
				if ovMsg := p.GetGangMemberMsg(targ); ovMsg != "" {
					/* Uh, oh, this is very ugly hack */

					if err := serv.SendShard(&pgproto3.Query{
						String: ovMsg,
					}, targ); err != nil {
						return err
					}

				} else {
					/* Assert for IsQuery here? */
					if err := serv.SendShard(qd.Msg, targ); err != nil {
						return err
					}
				}
			} else {
				/* Assert for IsQuery here? */
				if err := serv.SendShard(qd.Msg, targ); err != nil {
					return err
				}
			}

			guc, err := cl.FindBoolGUC(session.SPQR_LINEARIZE_DISPATCH)
			if err != nil {
				return err
			}

			if guc.Get(cl) && cl.ShowNoticeMsg() {

				_ = cl.ReplyNotice(fmt.Sprintf("dispatch prefetching results from shard %v", targ.Name))

				if err := serv.PrefetchResult(targ, 1); err != nil {
					return err
				}
			}

		} else {
			/* this message is actually bind */
			if err := serv.SendShard(qd.Msg, targ); err != nil {
				return err
			}

			if err := serv.SendShard(qd.exec, targ); err != nil {
				return err
			}

			if err := serv.SendShard(pgsync, targ); err != nil {
				return err
			}
		}
	}

	if cl.ShowNoticeMsg() && replyCl {
		_ = replyShardMatchesWithHosts(cl, serv, et)
	}
	return nil
}
