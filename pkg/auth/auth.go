package auth

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/xdg-go/scram"
	"golang.org/x/crypto/pbkdf2"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
)

// AuthBackend authenticates a user with the backend based on the provided message type.
//
// Parameters:
// - shard (conn.DBInstance): The database instance to authenticate against.
// - berule (*config.BackendRule): The backend rule containing authentication rules.
// - msg (pgproto3.BackendMessage): The authentication message.
//
// Returns:
// - error: An error if authentication fails or an unexpected message type is received.
//   - If the authentication method is `pgproto3.AuthenticationMD5Password`, the error message will be "auth rule not set for {shardName}-{berule.DB}-{berule.Usr}" if the rule is not found.
//   - If the authentication method is `pgproto3.AuthenticationCleartextPassword`, the error message will be "auth rule not set for {shardName}-{berule.DB}-{berule.Usr}" if the rule is not found.
//   - If the authentication method is `pgproto3.AuthenticationSASL`, the error message will depend on the specific error that occurs during the authentication process.
//   - For any other authentication method, the error message will be "authBackend type {type} not supported".
func AuthBackend(shard conn.DBInstance, berule *config.BackendRule, msg pgproto3.BackendMessage) error {
	switch v := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationMD5Password:

		var rule *config.AuthBackendCfg
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
		username := berule.Usr
		if rule.Usr != "" {
			username = rule.Usr
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
			hash.Write([]byte(rule.Password + username))
			res = hash.Sum(nil)
			res = []byte(hex.EncodeToString(res))
		}

		hashSalted := md5.New()
		hashSalted.Write(res)
		hashSalted.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		resSalted := hashSalted.Sum(nil)
		psswd := hex.EncodeToString(resSalted)

		return shard.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd})
	case *pgproto3.AuthenticationCleartextPassword:
		var rule *config.AuthBackendCfg
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
		var rule *config.AuthBackendCfg
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
		username := berule.Usr
		if rule.Usr != "" {
			username = rule.Usr
		}
		clientSHA256, err := scram.SHA256.NewClient(username, rule.Password, "")
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

// AuthFrontend handles authentication for the frontend based on the specified authentication method.
//
// Parameters:
// - cl (client.Client): - the client interface. It should implement the `Usr`, `PasswordCT`, and `PasswordMD5` methods.
// - rule (*config.FrontendRule): - the frontend rule configuration. It should contain the `AuthRule` field, which in turn should contain the `Method` field.
//
// Returns:
// - error: An error if authentication fails. The type of error returned depends on the authentication method used.
//   - If the authentication method is `config.AuthNotOK`, the error message will be "user {username} {database} blocked".
//   - If the authentication method is `config.AuthClearText`, the error message will be "user {username} {database} auth failed".
//   - If the authentication method is `config.AuthMD5`, the error message will be "[frontend_auth] route {username} {database}: md5 password mismatch".
//   - If the authentication method is `config.AuthSCRAM`, the error message will be "error: {error_message}".
func AuthFrontend(cl client.Client, rule *config.FrontendRule) error {
	switch rule.AuthRule.Method {
	case config.AuthOK:
		return nil
		// TODO:
	case config.AuthNotOK:
		return fmt.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
	case config.AuthClearText:
		if passwd, err := cl.PasswordCT(); err != nil || passwd != rule.AuthRule.Password {
			return fmt.Errorf("user %v %v auth failed", cl.Usr(), cl.DB())
		}
		return nil
	case config.AuthMD5:
		randBytes := make([]byte, 4)
		if _, err := rand.Read(randBytes); err != nil {
			return err
		}

		salt := [4]byte{randBytes[0], randBytes[1], randBytes[2], randBytes[3]}

		resp, err := cl.PasswordMD5(salt)
		if err != nil {
			return err
		}

		hash := md5.New()

		/* Accept encrypted version of passwd */
		if len(rule.AuthRule.Password) == 35 && rule.AuthRule.Password[0:3] == "md5" {
			hash.Write([]byte(rule.AuthRule.Password[3:]))
		} else {
			innerhash := md5.New()
			innerhash.Write([]byte(rule.AuthRule.Password + rule.Usr))
			innerres := innerhash.Sum(nil)
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
		const SCRAMSaltLen = 16
		const SCRAMIterCount = 4096
		const SCRAMKeyLen = 32
		salt := make([]byte, SCRAMSaltLen)
		if _, err := rand.Read(salt); err != nil {
			return err
		}
		saltedPassword := pbkdf2.Key([]byte(rule.AuthRule.Password), salt, SCRAMIterCount, SCRAMKeyLen, sha256.New)
		// Generate ServerKey = HMAC(saltedPassword, "Server Key")
		h := hmac.New(sha256.New, saltedPassword)
		h.Write([]byte("Server Key"))
		serverKey := h.Sum(nil)
		// Generate StoredKey = SHA256(HMAC(saltedPassword, "Client Key"))
		h.Reset()
		h.Write([]byte("Client Key"))
		clientKeyHash := sha256.New()
		clientKeyHash.Write(h.Sum(nil))
		storedKey := clientKeyHash.Sum(nil)
		serverSHA256, err := scram.SHA256.NewServer(
			func(username string) (scram.StoredCredentials, error) {
				return scram.StoredCredentials{
					KeyFactors: scram.KeyFactors{
						Salt:  string(salt),
						Iters: SCRAMIterCount,
					},
					ServerKey: serverKey,
					StoredKey: storedKey,
				}, nil
			})
		if err != nil {
			return err
		}
		conv := serverSHA256.NewConversation()
		var clientMsg string
		msg := &pgproto3.AuthenticationSASL{
			AuthMechanisms: []string{"SCRAM-SHA-256"},
		}
		if err = cl.Send(msg); err != nil {
			return err
		}
		if err = cl.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
			return err
		}
		clientMsgRaw, err := cl.Receive()
		if err != nil {
			return err
		}
		switch clientMsgRaw := clientMsgRaw.(type) {
		case *pgproto3.SASLInitialResponse:
			if clientMsgRaw.AuthMechanism != "SCRAM-SHA-256" {
				return fmt.Errorf("incorrect auth mechanism")
			}
			clientMsg = string(clientMsgRaw.Data)
		default:
			return fmt.Errorf("unexpected message type %T", clientMsgRaw)
		}
		secondMsg, err := conv.Step(clientMsg)
		if err != nil {
			return err
		}
		if err = cl.Send(&pgproto3.AuthenticationSASLContinue{
			Data: []byte(secondMsg),
		}); err != nil {
			return err
		}
		if err = cl.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
			return err
		}
		if clientMsgRaw, err = cl.Receive(); err != nil {
			return err
		}
		switch clientMsgRaw := clientMsgRaw.(type) {
		case *pgproto3.SASLResponse:
			clientMsg = string(clientMsgRaw.Data)
		default:
			return fmt.Errorf("unexpected message type %T", clientMsgRaw)
		}
		finalMsg, err := conv.Step(clientMsg)
		if err != nil {
			return err
		}
		err = cl.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte(finalMsg)})
		return err
	case config.AuthLDAP:
		if rule.AuthRule.LDAPConfig == nil {
			return fmt.Errorf("LDAP configuration is not set for ldap auth method")
		}

		conn, err := rule.AuthRule.LDAPConfig.ServerConn()
		if err != nil {
			return err
		}
		defer func() {
			if err := conn.Close(); err != nil {
				spqrlog.Zero.Info().Err(err).Msg("failed to close LDAP connection")
			}
		}()

		switch rule.AuthRule.LDAPConfig.AuthMode {
		case config.SimpleBindMode:
			password, err := cl.PasswordCT()
			if err != nil {
				return err
			}

			err = rule.AuthRule.LDAPConfig.SimpleBind(conn, cl.Usr(), password)
			if err != nil {
				return err
			}

			return nil
		case config.SearchAndBindMode:
			err = rule.AuthRule.LDAPConfig.SearchBind(conn)
			if err != nil {
				return err
			}

			searchAttribute := rule.AuthRule.LDAPConfig.ModifySearchAttribute()
			searchFilter := rule.AuthRule.LDAPConfig.ModifySearchFilter(searchAttribute, cl.Usr())

			searchResult, err := rule.AuthRule.LDAPConfig.DoSearchRequest(conn, searchFilter)
			if err != nil {
				return err
			}

			err = rule.AuthRule.LDAPConfig.CheckSearchResult(searchResult.Entries)
			if err != nil {
				return err
			}

			userDN := searchResult.Entries[0].DN
			password, err := cl.PasswordCT()
			if err != nil {
				return err
			}

			err = rule.AuthRule.LDAPConfig.SimpleBind(conn, userDN, password)
			if err != nil {
				return err
			}

			return nil
		default:
			return fmt.Errorf("invalid ldap auth mode '%v'", rule.AuthRule.LDAPConfig.AuthMode)
		}
	case config.AuthGSS:
		if rule.AuthRule.GssConfig == nil {
			return fmt.Errorf("GSS configuration are not set for GSS auth method")
		}
		if cl.Usr() != rule.Usr {
			return fmt.Errorf("user from client %v != %v mismatch user in config", cl.Usr(), rule.Usr)
		}
		b := BaseAuthModule{
			properties: map[string]any{
				keyTabFileProperty: rule.AuthRule.GssConfig.KrbKeyTabFile,
			},
		}
		kerb := NewKerberosModule(b)
		cred, err := kerb.Process(cl)
		if err != nil {
			return err
		}
		username := cred.UserName()
		if rule.AuthRule.GssConfig.IncludeRealm {
			username = fmt.Sprintf("%s@%s", cred.UserName(), cred.Realm())
		}
		if username != cl.Usr() {
			return fmt.Errorf("GSS username mismatch with pg user: '%v' != '%v'", username, cl.Usr())
		}
		if rule.AuthRule.GssConfig.KrbRealm != "" && rule.AuthRule.GssConfig.KrbRealm != cred.Realm() {
			return fmt.Errorf("GSS realm in token mismatch with realm in config: '%v' != '%v'", rule.AuthRule.GssConfig.KrbRealm, cred.Realm())
		}
		return nil
	default:
		return fmt.Errorf("invalid auth method '%v'", rule.AuthRule.Method)
	}
}
