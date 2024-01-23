package config

type LDAPCfg struct {
	LdapServer          string `json:"ldapserver" yaml:"ldapserver" toml:"ldapserver"`
	LdapPort            int    `json:"ldapport" yaml:"ldapport" toml:"ldapport"`
	LdapScheme          string `json:"ldapscheme" yaml:"ldapscheme" toml:"ldapscheme"`
	LdapBaseDn          string `json:"ldapbasedn" yaml:"ldapbasedn" toml:"ldapbasedn"`
	LdapBindDn          string `json:"ldapbinddn" yaml:"ldapbinddn" toml:"ldapbinddn"`
	LdapBindPasswd      string `json:"ldapbindpasswd" yaml:"ldapbindpasswd" toml:"ldapbindpasswd"`
	LdapSearchAttribute string `json:"ldapsearchattribute" yaml:"ldapsearchattribute" toml:"ldapsearchattribute"`
}
