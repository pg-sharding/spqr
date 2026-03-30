package testutil

import "github.com/jackc/pgx/v5"

func CurrenRowToMap(r pgx.Rows) (map[string]any, error) {
	values, err := r.Values()
	if err != nil {
		return nil, err
	}

	rowmap := make(map[string]any)
	ds := r.FieldDescriptions()
	for i, column := range ds {
		rowmap[column.Name] = values[i]
	}

	return rowmap, nil
}
