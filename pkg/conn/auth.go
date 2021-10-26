package conn

import (
	"crypto/md5"
	"encoding/hex"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pkg/errors"
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

		hash2 := md5.New()
		hash2.Write([]byte(hex.EncodeToString(res)))
		hash2.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		res2 := hash2.Sum(nil)

		psswd := hex.EncodeToString(res2)

		tracelog.InfoLogger.Printf("sending auth package %s plain passwd %s", psswd, cfg.Passwd)

		if err := shard.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd}); err != nil {
			return err
		}

	case *pgproto3.AuthenticationCleartextPassword:
		if err := shard.Send(&pgproto3.PasswordMessage{Password: cfg.Passwd}); err != nil {
			return err
		}
	default:
		return errors.Errorf("authBackend type %T not supported", msg)
	}

	return nil
}
