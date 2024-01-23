package config

type LDAPCfg struct {
	LdapServer          string `json:"ldapserver" yaml:"ldapserver" toml:"ldapserver"`
	LdapPort            string `json:"ldapport" yaml:"ldapport" toml:"ldapport"`
	LdapPrefix          string `json:"ldapprefix" yaml:"ldapprefix" toml:"ldapprefix"`
	LdapSuffix          string `json:"ldapsuffix" yaml:"ldapsuffix" toml:"ldapsuffix"`
	LdapBaseDn          string `json:"ldapbasedn" yaml:"ldapbasedn" toml:"ldapbasedn"`
	LdapBindDn          string `json:"ldapbinddn" yaml:"ldapbinddn" toml:"ldapbinddn"`
	LdapBindPasswd      string `json:"ldapbindpasswd" yaml:"ldapbindpasswd" toml:"ldapbindpasswd"`
	LdapSearchAttribute string `json:"ldapsearchattribute" yaml:"ldapsearchattribute" toml:"ldapsearchattribute"`
	LdapSearchFilter    string `json:"ldapsearchfilter" yaml:"ldapsearchfilter" toml:"ldapsearchfilter"`
	LdapUrl             string `json:"ldapurl" yaml:"ldapurl" toml:"ldapurl"`
}
