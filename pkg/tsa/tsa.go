package tsa

import (
	"fmt"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

/* target session attr utility */

func CheckTSA(sh shard.Shard) (bool, string, error) {
	if err := sh.Send(&pgproto3.Query{
		String: "SHOW transaction_read_only",
	}); err != nil {
		// spqrlog.Logger.Errorf("shard %s encounter error while sending read-write check %v", sh.Name(), err)
		return false, "", err
	}

	res := false
	reason := "zero datarow recieved"

	for {
		msg, err := sh.Receive()
		if err != nil {
			// spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p recieved error %v during check rw", sh, err)
			return false, reason, err
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p recieved %+v during check rw", sh, msg)
		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			// spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p checking read-write: result datarow %+v", sh, qt)
			if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
				res = true
			} else {
				reason = fmt.Sprintf("transaction_read_only is %+v", qt.Values)
			}

		case *pgproto3.ReadyForQuery:
			if txstatus.TXStatus(qt.TxStatus) != txstatus.TXIDLE {
				// spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p got unsync connection while calculating rw %v", sh, qt.TxStatus)
				return false, reason, fmt.Errorf("connection unsync while acquirind it")
			}

			// spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p calculated rw res %+v", sh, res)
			return res, reason, nil
		}
	}
}
