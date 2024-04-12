package config

const (
	UnixSocketDirectory = "/var/run/postgresql"
)

type AuthMethod string

const (
	AuthOK        = AuthMethod("ok")
	AuthNotOK     = AuthMethod("notok")
	AuthClearText = AuthMethod("clear_text")
	AuthMD5       = AuthMethod("md5")
	AuthSCRAM     = AuthMethod("scram")
)

type AuthCfg struct {
	Method   AuthMethod `json:"auth_method" yaml:"auth_method" toml:"auth_method"`
	Password string     `json:"password" yaml:"password" toml:"password"`
}
