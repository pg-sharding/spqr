package spqrparser_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresQueryHint(t *testing.T) {
	for _, query := range []string{
		"SELECT 1;",
		"INSERT INTO entries (id) VALUES (1);",
		"UPDATE entries SET id = 2;",
		"DELETE FROM entries WHERE id = 1;",
		"CREATE TABLE entries (id integer);",
		"WITH entries AS (SELECT 1) SELECT * FROM entries;",
		"/* application query */ SELECT 1;",
		"SELECT 1; SELECT 2;",
		"SHOW shards; SELECT 1;",
	} {
		t.Run(query, func(t *testing.T) {
			stmts, err := spqrparser.Parse(query)
			require.Nil(t, stmts)
			var spErr *spqrerror.SpqrError
			require.ErrorAs(t, err, &spErr)
			assert.Equal(t, spqrerror.PG_SYNTAX_ERROR, spErr.ErrorCode)
			assert.Contains(t, spErr.ErrHint, "application database")
			assert.Contains(t, spErr.ErrHint, "PostgreSQL port")
			assert.Contains(t, spErr.ErrHint, "spqr-console database or admin console port")
			assert.Positive(t, spErr.Position)
		})
	}
}

func TestInvalidQueryWithoutPostgresHint(t *testing.T) {
	for _, query := range []string{
		"SELECT * FROM",
		"INSERT INTO entries VALUES (",
		"SHOW clients extra;",
		"CREATE DISTRIBUTION ds1 COLUMN TYPES integer extra;",
	} {
		t.Run(query, func(t *testing.T) {
			stmts, err := spqrparser.Parse(query)
			require.Nil(t, stmts)
			var spErr *spqrerror.SpqrError
			require.ErrorAs(t, err, &spErr)
			assert.Equal(t, spqrerror.PG_SYNTAX_ERROR, spErr.ErrorCode)
			assert.Empty(t, spErr.ErrHint)
			assert.Positive(t, spErr.Position)
		})
	}
}

func TestConsoleCommandsTakePrecedenceOverPostgres(t *testing.T) {
	for _, query := range []string{"SHOW version;", "SHOW shards;"} {
		t.Run(query, func(t *testing.T) {
			stmts, err := spqrparser.Parse(query)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.IsType(t, &spqrparser.Show{}, stmts[0])
		})
	}
}
