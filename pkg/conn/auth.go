package conn

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
)

func AuthBackend(shard DBInstance, berule *config.BackendRule, msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "backend shard %p: auth type proc %T\n", shard, msg)

	switch v := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationMD5Password:
		if berule.AuthRule == nil {
			return fmt.Errorf("auth rule not set for %s-%s", berule.DB, berule.Usr)
		}
		var res []byte

		/* password may be configured in partially-calculated
		 * form to hide original passwd string
		 */
		if berule.AuthRule.Password[0:3] == "md5" {
			res = []byte(berule.AuthRule.Password)
		} else {
			hash := md5.New()
			hash.Write([]byte(berule.AuthRule.Password + berule.Usr))
			res = hash.Sum(nil)
		}

		hashSalted := md5.New()
		hashSalted.Write([]byte(hex.EncodeToString(res)))
		hashSalted.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		resSalted := hashSalted.Sum(nil)

		psswd := hex.EncodeToString(resSalted)

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "sending auth package %s plain passwd %s", psswd, berule.AuthRule.Password)
		return shard.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd})
	case *pgproto3.AuthenticationCleartextPassword:
		if berule.AuthRule == nil {
			return fmt.Errorf("no auth rule specified for server connection")
		}
		return shard.Send(&pgproto3.PasswordMessage{Password: berule.AuthRule.Password})
	default:
		return fmt.Errorf("authBackend type %T not supported", msg)
	}
}

func AuthFrontend(cl client.Client, authRule *config.AuthCfg) error {
	switch authRule.Method {
	case config.AuthOK:
		return nil
		// TODO:
	case config.AuthNotOK:
		return fmt.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
	case config.AuthClearText:
		if cl.PasswordCT() != authRule.Password {
			return fmt.Errorf("user %v %v auth failed", cl.Usr(), cl.DB())
		}
		return nil
	case config.AuthMD5:
		randBytes := make([]byte, 4)
		if _, err := rand.Read(randBytes); err != nil {
			return err
		}

		salt := [4]byte{randBytes[0], randBytes[1], randBytes[2], randBytes[3]}

		resp := cl.PasswordMD5(salt)

		hash := md5.New()
		hash.Write([]byte(authRule.Password))
		hash.Write([]byte{salt[0], salt[1], salt[2], salt[3]})
		saltedPasswd := hash.Sum(nil)

		token := "md5" + hex.EncodeToString(saltedPasswd)

		if resp != token {
			return fmt.Errorf("route %v %v: md5 password mismatch", cl.Usr(), cl.DB())
		}
		return nil
	case config.AuthSCRAM:
		fallthrough
	default:
		return fmt.Errorf("invalid auth method %v", authRule.Method)
	}
}
