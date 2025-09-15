package testutil

import "database/sql"

func CurrenRowToMap(r *sql.Rows) (map[string]interface{}, error) {
	rowmap := make(map[string]any)
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return nil, err
	}

	for i, column := range columns {
		rowmap[column] = *(values[i].(*interface{}))
	}

	return rowmap, nil
}
