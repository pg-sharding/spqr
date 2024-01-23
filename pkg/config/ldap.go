package config

type LDAPCfg struct {
	LdapServer          string `json:"ldapserver" yaml:"ldapserver" toml:"ldapserver"`
	LdapPort            int    `json:"ldapport" yaml:"ldapport" toml:"ldapport"`
	LdapScheme          string `json:"ldapscheme" yaml:"ldapscheme" toml:"ldapscheme"`
	LdapTLS             bool   `json:"ldaptls" yaml:"ldaptls" toml:"ldaptls"`
	LdapBaseDn          string `json:"ldapbasedn" yaml:"ldapbasedn" toml:"ldapbasedn"`
	LdapBindDn          string `json:"ldapbinddn" yaml:"ldapbinddn" toml:"ldapbinddn"`
	LdapBindPasswd      string `json:"ldapbindpasswd" yaml:"ldapbindpasswd" toml:"ldapbindpasswd"`
	LdapSearchAttribute string `json:"ldapsearchattribute" yaml:"ldapsearchattribute" toml:"ldapsearchattribute"`
	LdapSearchFilter    string `json:"ldapsearchfilter" yaml:"ldapsearchfilter" toml:"ldapsearchfilter"`
}
