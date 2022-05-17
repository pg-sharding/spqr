package conn

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
)

func AuthBackend(shard DBInstance, cfg *config.ShardCfg, msg pgproto3.BackendMessage) error {
	tracelog.InfoLogger.Printf("Auth type proc %+v\n", msg)

	switch v := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationMD5Password:

		hash := md5.New()
		hash.Write([]byte(cfg.Passwd + cfg.ConnUsr))
		res := hash.Sum(nil)

		hashSec := md5.New()
		hashSec.Write([]byte(hex.EncodeToString(res)))
		hashSec.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		res2 := hashSec.Sum(nil)

		psswd := hex.EncodeToString(res2)

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "sending auth package %s plain passwd %s", psswd, cfg.Passwd)

		return shard.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd})
	case *pgproto3.AuthenticationCleartextPassword:
		return shard.Send(&pgproto3.PasswordMessage{Password: cfg.Passwd})
	default:
		return fmt.Errorf("authBackend type %T not supported", msg)
	}
}
