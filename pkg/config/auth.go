package config

type AuthMethod string

const (
	AuthOK        = AuthMethod("ok")
	AuthNotOK     = AuthMethod("notok")
	AuthClearText = AuthMethod("clear_text")
	AuthMD5       = AuthMethod("md5")
	AuthSCRAM     = AuthMethod("scram")
	AuthLDAP      = AuthMethod("ldap")
)

type AuthCfg struct {
	Method     AuthMethod `json:"auth_method" yaml:"auth_method" toml:"auth_method"`
	Password   string     `json:"password" yaml:"password" toml:"password"`
	LDAPConfig *LDAPCfg   `json:"ldap_config" yaml:"ldap_config" toml:"ldap_config"`
}
