package conn

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
)

func AuthBackend(shard DBInstance, berule *config.BackendRule, msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "Auth type proc %T\n", msg)

	switch v := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationMD5Password:
		if berule.AuthRule == nil {
			return fmt.Errorf("no backend rule specified")
		}
		hash := md5.New()
		hash.Write([]byte(berule.AuthRule.Password + berule.Usr))
		res := hash.Sum(nil)

		hashSec := md5.New()
		hashSec.Write([]byte(hex.EncodeToString(res)))
		hashSec.Write([]byte{v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3]})
		res2 := hashSec.Sum(nil)

		psswd := hex.EncodeToString(res2)

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
