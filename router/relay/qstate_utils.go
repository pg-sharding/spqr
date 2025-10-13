package relay

import (
	"strings"

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

	if qd.P == nil {
		if err := serv.Send(qd.Msg); err != nil {
			return err
		}

		if cl.ShowNoticeMsg() && replyCl {
			_ = replyShardMatchesWithHosts(cl, serv, server.ServerShkeys(serv))
		}
	} else {
		et := qd.P.ExecutionTargets()

		if len(et) == 0 {
			if err := serv.Send(qd.Msg); err != nil {
				return err
			}

			if cl.ShowNoticeMsg() && replyCl {
				_ = replyShardMatchesWithHosts(cl, serv, server.ServerShkeys(serv))
			}
		} else {
			for _, targ := range et {
				if err := serv.SendShard(qd.Msg, targ); err != nil {
					return err
				}
			}

			if cl.ShowNoticeMsg() && replyCl {
				_ = replyShardMatchesWithHosts(cl, serv, et)
			}
		}
	}
	return nil
}
