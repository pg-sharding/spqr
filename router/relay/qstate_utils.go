package relay

import (
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
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

func DispatchPlan(qd *QueryDesc, serv server.Server, cl client.RouterClient, replyCl bool) error {

	shkey := server.ServerShkeys(serv)

	if qd.P == nil {
		if err := serv.Send(qd.Msg); err != nil {
			return err
		}

	} else {
		et := qd.P.ExecutionTargets()

		if len(et) == 0 {
			if err := serv.Send(qd.Msg); err != nil {
				return err
			}

		} else {
			for _, targ := range et {
				msg := qd.Msg
				if vmsg, ok := msg.(*pgproto3.Query); ok {
					if ovMsg := qd.P.GetGangMemberMsg(targ); ovMsg != "" {
						/* Uh, oh, this is very ugly hack */
						vmsg.String = ovMsg
					}
				}
				if err := serv.SendShard(msg, targ); err != nil {
					return err
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
