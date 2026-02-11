package testutil

import "database/sql"

func CurrenRowToMap(r *sql.Rows) (map[string]any, error) {
	rowmap := make(map[string]any)
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	err = r.Scan(values...)
	if err != nil {
		return nil, err
	}

	for i, column := range columns {
		rowmap[column] = *(values[i].(*any))
	}

	return rowmap, nil
}
