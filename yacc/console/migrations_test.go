package spqrparser_test

import (
	"testing"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/require"
)

func TestAlterSystemMigration(t *testing.T) {
	for _, tt := range []struct {
		query string
		want  *spqrparser.AlterSystemMigration
	}{
		{"ALTER SYSTEM MIGRATION SET init = abc123", &spqrparser.AlterSystemMigration{Name: "init", Value: "abc123"}},
		{"alter system migration set '0001_init' = '123456'", &spqrparser.AlterSystemMigration{Name: "0001_init", Value: "123456"}},
		{`ALTER SYSTEM MIGRATION SET "MixedCase" TO 'sha256:abc'`, &spqrparser.AlterSystemMigration{Name: "MixedCase", Value: "sha256:abc"}},
		{"ALTER SYSTEM MIGRATION RESET init", &spqrparser.AlterSystemMigration{Name: "init", Reset: true}},
		{"alter system migration reset '0001_init'", &spqrparser.AlterSystemMigration{Name: "0001_init", Reset: true}},
		{`ALTER SYSTEM MIGRATION RESET "MixedCase"`, &spqrparser.AlterSystemMigration{Name: "MixedCase", Reset: true}},
	} {
		t.Run(tt.query, func(t *testing.T) {
			statements, err := spqrparser.Parse(tt.query + ";")
			require.NoError(t, err)
			require.Equal(t, []spqrparser.Statement{&spqrparser.Alter{Element: tt.want}}, statements)
		})
	}
	for _, sql := range []string{
		"ALTER SYSTEM MIGRATION SET init",
		"ALTER SYSTEM MIGRATION SET init =",
		"ALTER SYSTEM MIGRATION RESET",
		"ALTER SYSTEM MIGRATION RESET init = 'checksum'",
		"ALTER SYSTEM MIGRATION RESET ALL",
		"ALTER SYSTEM MIGRATION init = 'checksum'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := spqrparser.Parse(sql)
			require.Error(t, err)
		})
	}
}

func TestMigrationKeywordsAsSettings(t *testing.T) {
	statements, err := spqrparser.Parse("ALTER SYSTEM SET migration = reset;")
	require.NoError(t, err)
	require.Equal(t, &spqrparser.Alter{Element: &spqrparser.System{SetGUC: "migration", SetValue: "reset"}}, statements[0])
}

func TestMigrationHelpSyntax(t *testing.T) {
	for _, command := range []string{"ALTER SYSTEM MIGRATION", "ALTER SYSTEM MIGRATION SET", "ALTER SYSTEM MIGRATION RESET"} {
		statements, err := spqrparser.Parse("HELP " + command)
		require.NoError(t, err)
		require.Equal(t, &spqrparser.Help{CommandName: command}, statements[0])
	}
}
