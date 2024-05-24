package config

type GssCfg struct {
	KrbKeyTabFile string `json:"krb_keytab_file" yaml:"krb_keytab_file" toml:"krb_keytab_file"`
	KrbRealm      string `json:"krb_realm" yaml:"krb_realm" toml:"krb_realm"`
	IncludeRealm  bool   `json:"include_realm" yaml:"include_realm" toml:"include_realm"`
}
