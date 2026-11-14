package qrouter

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
)

func TestStripWithTies(t *testing.T) {
	for _, tc := range []struct {
		query, want string
		wantError   bool
	}{
		{
			query: "SELECT 1 ORDER BY 1 OFFSET 1 /* gap */ FETCH FIRST 2 ROWS WITH TIES /* tail */;",
			want:  "SELECT 1 ORDER BY 1  /* tail */;",
		},
		{
			query: "SELECT 1 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES /* gap */ OFFSET 1 ROWS /* tail */;",
			want:  "SELECT 1 ORDER BY 1  /* tail */;",
		},
		{
			query:     "SELECT 1 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES FOR UPDATE",
			wantError: true,
		},
		{
			query:     "SELECT 1 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES; SELECT 2",
			wantError: true,
		},
		{
			query:     "SELECT 1 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES OFFSET 1+1",
			wantError: true,
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			statements, _, err := lyx.Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			got, err := stripWithTies(tc.query, statements[0].(*lyx.Select))
			if (err != nil) != tc.wantError {
				t.Fatalf("stripWithTies error = %v, wantError = %v", err, tc.wantError)
			}
			if !tc.wantError && got != tc.want {
				t.Fatalf("stripWithTies = %q, want %q", got, tc.want)
			}
		})
	}
}
