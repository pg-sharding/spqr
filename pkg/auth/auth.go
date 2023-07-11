package auth

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
)

func AuthBackend(shard conn.DBInstance, berule *config.BackendRule, msg pgproto3.BackendMessage) error {
	spqrlog.Zero.Debug().
		Uint("shard ", spqrlog.GetPointer(shard)).
		Type("authtype", msg).
		Msg("auth backend")

	switch v := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationMD5Password:
		if berule.AuthRules == nil {
			return fmt.Errorf("auth rule not set for %s-%s", berule.DB, berule.Usr)
		}
		if _, exists := berule.AuthRules[shard.ShardName()]; !exists {
			return fmt.Errorf("auth rule not set for %s-%s-%s", shard.ShardName(), berule.DB, berule.Usr)
		}
		var res []byte

		/* password may be configured in partially-calculated
		 * form to hide original passwd string
		 */
		/*35=len("md5") + 2  * 16*/
		if len(berule.AuthRules[shard.ShardName()].Password) == 35 && berule.AuthRules[shard.ShardName()].Password[0:3] == "md5" {
			res = []byte(berule.AuthRules[shard.ShardName()].Password[3:])
		} else {
			hash := md5.New()
			hash.Write([]byte(berule.AuthRules[shard.ShardName()].Password + berule.Usr))
			res = hash.Sum(nil)
			res = []byte(hex.EncodeToString(res))
		}

		hashSalted := md5.New()
		hashSalted.Write(res)
		hashSalted.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		resSalted := hashSalted.Sum(nil)

		psswd := hex.EncodeToString(resSalted)

		spqrlog.Zero.Debug().
			Str("password1", psswd).
			Str("password2", berule.AuthRules[shard.ShardName()].Password).
			Msg("sending plain password auth package")

		return shard.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd})
	case *pgproto3.AuthenticationCleartextPassword:
		if berule.AuthRules == nil {
			return fmt.Errorf("no auth rule specified for server connection")
		}
		if _, exists := berule.AuthRules[shard.ShardName()]; !exists {
			return fmt.Errorf("auth rule not set for %s-%s-%s", shard.ShardName(), berule.DB, berule.Usr)
		}
		return shard.Send(&pgproto3.PasswordMessage{Password: berule.AuthRules[shard.ShardName()].Password})
	default:
		return fmt.Errorf("authBackend type %T not supported", msg)
	}
}

func AuthFrontend(cl client.Client, rule *config.FrontendRule) error {
	switch rule.AuthRule.Method {
	case config.AuthOK:
		return nil
		// TODO:
	case config.AuthNotOK:
		return fmt.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
	case config.AuthClearText:
		if cl.PasswordCT() != rule.AuthRule.Password {
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

		/* Accept encrypted version of passwd */
		if len(rule.AuthRule.Password) == 35 && rule.AuthRule.Password[0:3] == "md5" {
			hash.Write([]byte(rule.AuthRule.Password[3:]))
		} else {
			innerhash := md5.New()
			innerhash.Write([]byte(rule.AuthRule.Password + rule.Usr))
			innerres := innerhash.Sum(nil)
			spqrlog.Zero.Debug().Bytes("inner-hash", innerres).Msg("")
			hash.Write([]byte(hex.EncodeToString(innerres)))
		}
		hash.Write([]byte{salt[0], salt[1], salt[2], salt[3]})
		saltedPasswd := hash.Sum(nil)

		token := "md5" + hex.EncodeToString(saltedPasswd)

		if resp != token {
			return fmt.Errorf("[frontend_auth] route %v %v: md5 password mismatch", cl.Usr(), cl.DB())
		}
		return nil
	case config.AuthSCRAM:
		fallthrough
	default:
		return fmt.Errorf("invalid auth method %v", rule.AuthRule.Method)
	}
}
