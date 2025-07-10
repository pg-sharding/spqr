package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModifyQuery(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		name     string
		query    string
		colname  string
		nextval  int64
		expected string
		wantErr  bool
	}

	for _, tt := range []tcase{
		{
			name:     "InsertWithColumns",
			query:    "INSERT INTO test_table (col1, col2) VALUES (1, 2);",
			colname:  "col3",
			nextval:  42,
			expected: "INSERT INTO test_table (col3, col1, col2) VALUES (42, 1, 2);",
			wantErr:  false,
		},
		{
			name:     "InsertWithoutColumns",
			query:    "INSERT INTO test_table VALUES (1, 2);",
			colname:  "col3",
			nextval:  42,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertEmptyValues",
			query:    "INSERT INTO test_table (col1, col2) VALUES;",
			colname:  "col3",
			nextval:  42,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertSingleColumn",
			query:    "INSERT INTO test_table (col1) VALUES (1);",
			colname:  "col2",
			nextval:  99,
			expected: "INSERT INTO test_table (col2, col1) VALUES (99, 1);",
			wantErr:  false,
		},
		{
			name:     "NotAnInsertStatement",
			query:    "SELECT 1",
			colname:  "col2",
			nextval:  99,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Comment",
			query:    "--ping;",
			colname:  "col2",
			nextval:  99,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertWithReturningClause",
			query:    "INSERT INTO meta.campaigns_ocb_rate (campaign_id, rates, updated_at) VALUES ($1, $2, $3::timestamp) RETURNING meta.campaigns_ocb_rate.id;",
			colname:  "created_at",
			nextval:  1234567890,
			expected: "INSERT INTO meta.campaigns_ocb_rate (created_at, campaign_id, rates, updated_at) VALUES (1234567890, $1, $2, $3::timestamp) RETURNING meta.campaigns_ocb_rate.id;",
			wantErr:  false,
		},
		{
			name:     "InsertWithArrayAndReturningClause",
			query:    `INSERT INTO meta.strategies (campaign_id, strategy_template_id, order_num, weight, variables, name, is_deleted, target_groups, target_groups_exclude, dynamic_params, updated_at, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, ARRAY[$8]::INTEGER[], ARRAY[$9]::INTEGER[], $10, $11::timestamp, $12::timestamp) RETURNING meta.strategies.id;`,
			colname:  "created_by",
			nextval:  987654321,
			expected: `INSERT INTO meta.strategies (created_by, campaign_id, strategy_template_id, order_num, weight, variables, name, is_deleted, target_groups, target_groups_exclude, dynamic_params, updated_at, created_at) VALUES (987654321, $1, $2, $3, $4, $5, $6, $7, ARRAY[$8]::INTEGER[], ARRAY[$9]::INTEGER[], $10, $11::timestamp, $12::timestamp) RETURNING meta.strategies.id;`,
			wantErr:  false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ModifyQuery(tt.query, tt.colname, tt.nextval)

			if tt.wantErr && err == nil {

				t.Errorf("ModifyQuery/%s expected error, got nil. Query: %s", tt.name, result)
				return
			}

			if err != nil {
				if (err != nil) != tt.wantErr {
					t.Errorf("ModifyQuery/%s error = %v, wantErr %v", tt.name, err, tt.wantErr)
				}
				return
			}

			assert.Equal(tt.expected, result)
		})
	}
}

func BenchmarkModifyQuery(b *testing.B) {
	query := "INSERT INTO table_name (col1, col3) VALUES (1, 3);"
	colname := "col2"
	nextval := int64(42)

	for i := 0; i < b.N; i++ {
		_, _ = ModifyQuery(query, colname, nextval)
	}
}
