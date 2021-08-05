package core

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgproto3"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type connAuth struct {
}

func authBackend(sh *ShServer, v *pgproto3.Authentication, shardConnCfg ConnConnCfg) error {

	fmt.Printf("Auth type proc %T\n", v)
	switch v.Type {
	case pgproto3.AuthTypeOk:
		return nil
	case pgproto3.AuthTypeMD5Password:
		hash := md5.New()

		hash.Write([]byte(shardConnCfg.Passwd + shardConnCfg.ConnUsr))

		res := hash.Sum(nil)

		res = append(res, v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3])
		tracelog.InfoLogger.Println("authBackend bypass md5 %s", res)

		str1 := hex.EncodeToString(res)

		hash2 := md5.New()
		hash2.Write([]byte(str1))

		res2 := hash2.Sum(nil)

		psswd := hex.EncodeToString(res2)

		tracelog.InfoLogger.Println("authBackend bypass md5 %s", psswd)

		if err := sh.fr.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd}); err != nil {
			return err
		}

	case pgproto3.AuthTypeCleartextPassword:
		tracelog.InfoLogger.Println("authBackend bypass %s", shardConnCfg.Passwd)
		if err := sh.fr.Send(&pgproto3.PasswordMessage{Password: shardConnCfg.Passwd}); err != nil {
			return err
		}
	default:
		return xerrors.Errorf("authBackend type %T not supportes", v.Type)
	}

	return nil
}
