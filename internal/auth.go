package internal

import (
	"crypto/md5"
	"encoding/hex"

	"github.com/jackc/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

func authBackend(sh *SpqrServer, v *pgproto3.Authentication) error {
	tracelog.InfoLogger.Printf("Auth type proc %+v\n", v)
	switch v.Type {
	case pgproto3.AuthTypeOk:
		return nil
	case pgproto3.AuthTypeMD5Password:
		hash := md5.New()
		hash.Write([]byte(sh.rule.SHStorage.Passwd + sh.rule.SHStorage.ConnUsr))
		res := hash.Sum(nil)

		hash2 := md5.New()
		hash2.Write([]byte(hex.EncodeToString(res)))
		hash2.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		res2 := hash2.Sum(nil)

		psswd := hex.EncodeToString(res2)
		if err := sh.frontend.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd}); err != nil {
			return err
		}

	case pgproto3.AuthTypeCleartextPassword:
		if err := sh.frontend.Send(&pgproto3.PasswordMessage{Password: sh.rule.SHStorage.Passwd}); err != nil {
			return err
		}
	default:
		return errors.Errorf("authBackend type %T not supported", v.Type)
	}

	return nil
}
