package relay

import (
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/server"
)

func virtualParamTransformName(name string) string {
	retName := name
	if after, ok := strings.CutPrefix(retName, "__spqr__."); ok {
		retName = "__spqr__" + after
	}

	return retName
}

func DispatchSlice(qd *QueryDesc,
	P plan.Plan, cl client.RouterClient, replyCl bool) error {

	serv := cl.Server()

	shkey := server.ServerShkeys(serv)

	if P == nil {

		if qd.simple {
			if err := serv.Send(qd.Msg); err != nil {
				return err
			}
		} else {

			/* this message is actually bind */
			if err := serv.Send(qd.Msg); err != nil {
				return err
			}

			if err := serv.Send(qd.exec); err != nil {
				return err
			}

			if err := serv.Send(pgsync); err != nil {
				return err
			}
		}

	} else {
		et := P.ExecutionTargets()

		if len(et) == 0 {

			if qd.simple {
				if err := serv.Send(qd.Msg); err != nil {
					return err
				}
			} else {

				/* this message is actually bind */
				if err := serv.Send(qd.Msg); err != nil {
					return err
				}

				if err := serv.Send(qd.exec); err != nil {
					return err
				}

				if err := serv.Send(pgsync); err != nil {
					return err
				}
			}

		} else {
			for _, targ := range et {

				if qd.simple {

					/*
					* This is only execution patch for non-top level slice
					 */

					if ovMsg := P.GetGangMemberMsg(targ); ovMsg != "" {
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
			shkey = et
		}
	}

	if cl.ShowNoticeMsg() && replyCl {
		_ = replyShardMatchesWithHosts(cl, serv, shkey)
	}
	return nil
}
