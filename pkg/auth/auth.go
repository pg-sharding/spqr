package auth

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/xdg-go/scram"

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

		var rule *config.AuthCfg
		if berule.AuthRules == nil {
			rule = berule.DefaultAuthRule
		} else if _, exists := berule.AuthRules[shard.ShardName()]; exists {
			rule = berule.AuthRules[shard.ShardName()]
		} else {
			rule = berule.DefaultAuthRule
		}

		if rule == nil {
			return fmt.Errorf("auth rule not set for %s-%s-%s", shard.ShardName(), berule.DB, berule.Usr)
		}

		var res []byte

		/* password may be configured in partially-calculated
		 * form to hide original passwd string
		 */
		/*35=len("md5") + 2  * 16*/
		if len(rule.Password) == 35 && rule.Password[0:3] == "md5" {
			res = []byte(rule.Password[3:])
		} else {
			hash := md5.New()
			hash.Write([]byte(rule.Password + berule.Usr))
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
		var rule *config.AuthCfg
		if berule.AuthRules == nil {
			rule = berule.DefaultAuthRule
		} else if _, exists := berule.AuthRules[shard.ShardName()]; exists {
			rule = berule.AuthRules[shard.ShardName()]
		} else {
			rule = berule.DefaultAuthRule
		}

		if rule == nil {
			return fmt.Errorf("auth rule not set for %s-%s-%s", shard.ShardName(), berule.DB, berule.Usr)
		}

		return shard.Send(&pgproto3.PasswordMessage{Password: rule.Password})
	case *pgproto3.AuthenticationSASL:
		var rule *config.AuthCfg
		if berule.AuthRules == nil {
			rule = berule.DefaultAuthRule
		} else if _, exists := berule.AuthRules[shard.ShardName()]; exists {
			rule = berule.AuthRules[shard.ShardName()]
		} else {
			rule = berule.DefaultAuthRule
		}
		clientSHA256, err := scram.SHA256.NewClient(berule.Usr, rule.Password, "")
		if err != nil {
			return err
		}

		conv := clientSHA256.NewConversation()
		var serverMsg string

		firstMsg, err := conv.Step(serverMsg)
		if err != nil {
			return err
		}

		if err = shard.Send(&pgproto3.SASLInitialResponse{
			AuthMechanism: "SCRAM-SHA-256",
			Data:          []byte(firstMsg),
		}); err != nil {
			return err
		}
		serverMsgRaw, err := shard.Receive()
		if err != nil {
			return err
		}
		switch serverMsgRaw := serverMsgRaw.(type) {
		case *pgproto3.AuthenticationSASLContinue:
			serverMsg = string(serverMsgRaw.Data)
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error: %s", serverMsgRaw.Message)
		default:
			return fmt.Errorf("unexpected server message type: %T", serverMsgRaw)
		}

		secondMsg, err := conv.Step(serverMsg)
		if err != nil {
			return err
		}
		if err = shard.Send(&pgproto3.SASLResponse{Data: []byte(secondMsg)}); err != nil {
			return err
		}
		serverMsgRaw, err = shard.Receive()
		if err != nil {
			return err
		}
		switch serverMsgRaw := serverMsgRaw.(type) {
		case *pgproto3.AuthenticationSASLFinal:
			serverMsg = string(serverMsgRaw.Data)
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error: %s", serverMsgRaw.Message)
		default:
			return fmt.Errorf("unexpected server message type: %T", serverMsgRaw)
		}

		_, err = conv.Step(serverMsg)
		return err
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
