package core

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgproto3"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type AuthMethod string

const AuthOK = AuthMethod("ok")
const AuthNOTOK = AuthMethod("notok")
const AuthClearText = AuthMethod("clear_text")
const AuthMD5 = AuthMethod("md5")
const AuthSASL = AuthMethod("scram")

type AuthRule struct {
	Am AuthMethod `json:"auth_method" yaml:"auth_method" toml:"auth_method"`
	Password string `json:"password" yaml:"password" toml:"password"`
}

func authBackend(sh *ShServer, v *pgproto3.Authentication) error {

	fmt.Printf("Auth type proc %T\n", v)
	switch v.Type {
	case pgproto3.AuthTypeOk:
		return nil
	case pgproto3.AuthTypeMD5Password:
		hash := md5.New()

		hash.Write([]byte(sh.rule.SHStorage.Passwd + sh.rule.SHStorage.ConnUsr))

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
		tracelog.InfoLogger.Println("authBackend bypass %s", sh.rule.SHStorage.Passwd)
		if err := sh.fr.Send(&pgproto3.PasswordMessage{Password: sh.rule.SHStorage.Passwd}); err != nil {
			return err
		}
	default:
		return xerrors.Errorf("authBackend type %T not supportes", v.Type)
	}

	return nil
}
