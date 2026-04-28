package auth

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/service"
	"github.com/pg-sharding/spqr/pkg/client"
)

type BaseAuthModule struct {
	properties map[string]any
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

func NewKerberosModule(base BaseAuthModule) (*Kerberos, error) {
	k := &Kerberos{
		BaseAuthModule: base,
	}
	var kt *keytab.Keytab
	var err error
	if ktFileProp, ok := k.properties[keyTabFileProperty]; ok {
		ktFile, ok := ktFileProp.(string)
		if !ok {
			return nil, fmt.Errorf("keytabfile property is not a string")
		}
		kt, err = keytab.Load(ktFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load keytab file: %w", err)
		}
	} else if ktDataProp, ok := k.properties[keyTabDataProperty]; ok {
		ktData, ok := ktDataProp.(string)
		if !ok {
			return nil, fmt.Errorf("keytabdata property is not a string")
		}
		b, err := hex.DecodeString(ktData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode keytab data: %w", err)
		}
		kt = keytab.New()
		err = kt.Unmarshal(b)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal keytab: %w", err)
		}
	}
	k.kt = kt
	if spProp, ok := k.properties[servicePrincipalProperty]; ok {
		servicePrincipal, ok := spProp.(string)
		if !ok {
			return nil, fmt.Errorf("serviceprincipal property is not a string")
		}
		k.servicePrincipal = servicePrincipal
	}

	return k, nil
}

func (k *Kerberos) Process(cl client.Client) (cred *credentials.Credentials, err error) {
	kt := k.kt
	if err != nil {
		panic(err) // If the "krb5.keytab" file is not available the application will show an error message.
	}
	settings := service.NewSettings(kt)
	msg := &pgproto3.AuthenticationGSS{}
	if err := cl.Send(msg); err != nil {
		return nil, err
	}
	if err := cl.SetAuthType(pgproto3.AuthTypeGSS); err != nil {
		return nil, err
	}

	st := KRB5Token{
		settings: settings,
	}
	clientMsgRaw, err := cl.Receive()
	if err != nil {
		return nil, err
	}
	switch clientMsgRaw := clientMsgRaw.(type) {
	case *pgproto3.GSSResponse:
		err := st.Unmarshal(clientMsgRaw.Data)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected message type %T", clientMsgRaw)
	}
	// Validate the context token
	authed, status := st.Verify()
	if status.Code != gssapi.StatusComplete && status.Code != gssapi.StatusContinueNeeded {
		errText := fmt.Sprintf("Kerberos validation error: %v", status)
		log.Print(errText)
		return nil, fmt.Errorf("%s", errText)
	}
	if status.Code == gssapi.StatusContinueNeeded {
		errText := "Kerberos GSS-API continue needed"
		log.Print(errText)
		return nil, fmt.Errorf("%s", errText)
	}
	if authed {
		ctx := st.Context()
		id, ok := ctx.Value(CtxCredential).(*credentials.Credentials)
		if !ok {
			return nil, fmt.Errorf("context credential is not of type *credentials.Credentials")
		}
		return id, nil
	}
	errText := "Kerberos authentication failed"
	log.Print(errText)
	return nil, fmt.Errorf("%s", errText)
}
