package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

type LDAPAuthMode string
type LDAPConnMode string
type LDAPTLSVerifyMode string

const (
	SimpleBindMode    = LDAPAuthMode("simple_bind")
	SearchAndBindMode = LDAPAuthMode("search_and_bind")

	UnencryptedMode = LDAPConnMode("unencrypted")
	SchemeMode      = LDAPConnMode("scheme")
	StartTLSMode    = LDAPConnMode("start_tls")

	OneWayMode = LDAPTLSVerifyMode("one_way")
	TwoWayMode = LDAPTLSVerifyMode("two_way")
)

var (
	ErrServerConn   = errors.New("there are no ldap servers available or such servers don't exist")
	ErrSearchResult = errors.New("too many entries returned or user doesn't exist")
)

type LDAPCfg struct {
	AuthMode   LDAPAuthMode `json:"ldap_auth_mode" yaml:"ldap_auth_mode" toml:"ldap_auth_mode"`
	ConnConfig *LDAPConnCfg `json:"ldap_conn_config" yaml:"ldap_conn_config" toml:"ldap_conn_config"`
	Servers    []string     `json:"servers" yaml:"servers" toml:"servers"`

	Prefix string `json:"prefix" yaml:"prefix" toml:"prefix"`
	Suffix string `json:"suffix" yaml:"suffix" toml:"suffix"`

	BindDN          string `json:"bind_dn" yaml:"bind_dn" toml:"bind_dn"`
	BindPassword    string `json:"bind_password" yaml:"bind_password" toml:"bind_password"`
	BaseDN          string `json:"base_dn" yaml:"base_dn" toml:"base_dn"`
	SearchAttribute string `json:"search_attribute" yaml:"search_attribute" toml:"search_attribute"`
	SearchFilter    string `json:"search_filter" yaml:"search_filter" toml:"search_filter"`
}

type LDAPConnCfg struct {
	ConnMode  LDAPConnMode `json:"ldap_conn_mode" yaml:"ldap_conn_mode" toml:"ldap_conn_mode"`
	Scheme    string       `json:"scheme" yaml:"scheme" toml:"scheme"`
	Port      string       `json:"port" yaml:"port" toml:"port"`
	TLSConfig *LDAPTLSCfg  `json:"ldap_tls_config" yaml:"ldap_tls_config" toml:"ldap_tls_config"`
}

type LDAPTLSCfg struct {
	TLSVerifyMode LDAPTLSVerifyMode `json:"tls_verify_mode" yaml:"tls_verify_mode" toml:"tls_verify_mode"`
	RootCertFile  string            `json:"root_cert_file" toml:"root_cert_file" yaml:"root_cert_file"`
	CertFile      string            `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
	KeyFile       string            `json:"key_file" toml:"key_file" yaml:"key_file"`
}

func (l *LDAPCfg) ServerConn() (*ldap.Conn, error) {
	switch l.ConnConfig.ConnMode {
	case UnencryptedMode, SchemeMode, StartTLSMode:
		for _, server := range l.Servers {
			conn, err := ldap.DialURL(l.ldapUrl(server))
			if err != nil {
				continue
			}

			if l.ConnConfig.ConnMode == StartTLSMode {
				err = l.startTLS(conn, server)
				if err != nil {
					return nil, err
				}
			}

			return conn, nil
		}

		return nil, ErrServerConn
	default:
		return nil, fmt.Errorf("invalid ldap connection mode '%v'", l.ConnConfig.ConnMode)
	}
}

func (l *LDAPCfg) SimpleBind(conn *ldap.Conn, username string, password string) error {
	err := conn.Bind(l.ldapUsername(username), password)
	if err != nil {
		return err
	}

	return nil
}

func (l *LDAPCfg) SearchBind(conn *ldap.Conn) error {
	var err error

	if l.BindDN == "" || l.BindPassword == "" {
		err = conn.UnauthenticatedBind("")
	} else {
		err = l.SimpleBind(conn, l.BindDN, l.BindPassword)
	}
	if err != nil {
		return err
	}

	return nil
}

func (l *LDAPCfg) ModifySearchAttribute() string {
	switch l.SearchAttribute {
	case "":
		return "uid"
	default:
		return l.SearchAttribute
	}
}

func (l *LDAPCfg) ModifySearchFilter(searchAttribute string, username string) string {
	switch l.SearchFilter {
	case "":
		return fmt.Sprintf("(%s=%s)", l.SearchAttribute, ldap.EscapeFilter(username))
	default:
		return strings.ReplaceAll(l.SearchFilter, "$username", ldap.EscapeFilter(username))
	}
}

func (l *LDAPCfg) DoSearchRequest(conn *ldap.Conn, searchFilter string) (*ldap.SearchResult, error) {
	searchRequest := ldap.NewSearchRequest(
		l.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		0,
		0,
		false,
		searchFilter,
		[]string{"dn"},
		nil,
	)

	searchResult, err := conn.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	return searchResult, nil
}

func (l *LDAPCfg) CheckSearchResult(entries []*ldap.Entry) error {
	if len(entries) != 1 {
		return ErrSearchResult
	}

	return nil
}

func (l *LDAPCfg) startTLS(conn *ldap.Conn, server string) error {
	switch l.ConnConfig.TLSConfig.TLSVerifyMode {
	case OneWayMode:
		caCertPool, err := l.loadCaCertPool()
		if err != nil {
			return err
		}

		err = conn.StartTLS(&tls.Config{
			ServerName: server,
			RootCAs:    caCertPool,
		})
		if err != nil {
			return err
		}

		return nil
	case TwoWayMode:
		cert, err := tls.LoadX509KeyPair(l.ConnConfig.TLSConfig.CertFile, l.ConnConfig.TLSConfig.KeyFile)
		if err != nil {
			return err
		}

		caCertPool, err := l.loadCaCertPool()
		if err != nil {
			return err
		}

		err = conn.StartTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
			ServerName:   server,
		})
		if err != nil {
			return err
		}

		return nil
	default:
		return fmt.Errorf("invalid ldap tls mode '%v'", l.ConnConfig.TLSConfig.TLSVerifyMode)
	}
}

func (l *LDAPCfg) loadCaCertPool() (*x509.CertPool, error) {
	caCert, err := os.ReadFile(l.ConnConfig.TLSConfig.RootCertFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return caCertPool, nil
}

func (l *LDAPCfg) ldapUrl(server string) string {
	return fmt.Sprintf("%s://%s:%s", l.ConnConfig.Scheme, server, l.ConnConfig.Port)
}

func (l *LDAPCfg) ldapUsername(username string) string {
	return fmt.Sprintf("%s%s%s", l.Prefix, username, l.Suffix)
}
