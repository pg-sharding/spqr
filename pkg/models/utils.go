package models

func ConvertMany[X any, Y any](xs []X, convert func(X) Y) []Y {
	ys := make([]Y, len(xs))
	for i, x := range xs {
		ys[i] = convert(x)
	}
	return ys
}

func ConvertManyWithError[X any, Y any](xs []X, convert func(X) (Y, error)) ([]Y, error) {
	ys := make([]Y, len(xs))
	var err error
	for i, x := range xs {
		ys[i], err = convert(x)
		if err != nil {
			return nil, err
		}
	}
	return ys, nil
}
