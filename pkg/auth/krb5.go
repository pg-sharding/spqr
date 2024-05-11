package auth

import (
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jcmturner/goidentity/v6"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/service"
	"github.com/pg-sharding/spqr/pkg/client"
	"log"
)

const ctxCredentials = "github.com/jcmturner/gokrb5/v8/ctxCredentials"

type BaseAuthModule struct {
	properties map[string]interface{}
}
type Kerberos struct {
	BaseAuthModule
	servicePrincipal string
	kt               *keytab.Keytab
}

const (
	keyTabFileProperty       = "keytabfile"
	keyTabDataProperty       = "keytabdata"
	servicePrincipalProperty = "serviceprincipal"
)

func NewKerberosModule(base BaseAuthModule) *Kerberos {
	k := &Kerberos{
		BaseAuthModule: base,
	}
	var kt *keytab.Keytab
	var err error
	if ktFileProp, ok := k.BaseAuthModule.properties[keyTabFileProperty]; ok {
		ktFile, _ := ktFileProp.(string)
		kt, err = keytab.Load(ktFile)
		if err != nil {
			panic(err) // If the "krb5.keytab" file is not available the application will show an error message.
		}
	} else if ktDataProp, ok := k.BaseAuthModule.properties[keyTabDataProperty]; ok {
		ktData := ktDataProp.(string)
		b, _ := hex.DecodeString(ktData)
		kt = keytab.New()
		err = kt.Unmarshal(b)
		if err != nil {
			panic(err)
		}
	}
	k.kt = kt
	if spProp, ok := k.BaseAuthModule.properties[servicePrincipalProperty]; ok {
		k.servicePrincipal = spProp.(string)
	}

	return k
}

func (k *Kerberos) Process(cl client.Client) (username string, err error) {
	kt := k.kt
	if err != nil {
		panic(err) // If the "krb5.keytab" file is not available the application will show an error message.
	}
	settings := service.NewSettings(kt)
	msg := &pgproto3.AuthenticationGSS{}
	if err := cl.Send(msg); err != nil {
		return "", err
	}
	if err := cl.SetAuthType(pgproto3.AuthTypeGSS); err != nil {
		return "", err
	}

	st := KRB5Token{
		settings: settings,
	}
	clientMsgRaw, err := cl.Receive()
	if err != nil {
		return "", err
	}
	switch clientMsgRaw := clientMsgRaw.(type) {
	case *pgproto3.GSSResponse:
		err := st.Unmarshal(clientMsgRaw.Data)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unexpected message type %T", clientMsgRaw)
	}
	// Validate the context token
	authed, status := st.Verify()
	if status.Code != gssapi.StatusComplete && status.Code != gssapi.StatusContinueNeeded {
		errText := fmt.Sprintf("Kerberos validation error: %v", status)
		log.Print(errText)
		return "", fmt.Errorf(errText)
	}
	if status.Code == gssapi.StatusContinueNeeded {
		errText := "Kerberos GSS-API continue needed"
		log.Print(errText)
		return "", fmt.Errorf(errText)
	}
	if authed {
		ctx := st.Context()
		id := ctx.Value(ctxCredentials).(goidentity.Identity)
		log.Print(id.UserName())
		return id.UserName(), nil
	} else {
		errText := "Kerberos authentication failed"
		log.Print(errText)
		return "", fmt.Errorf(errText)
	}
}
