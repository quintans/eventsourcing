package slices

// Map maps a slice of values from one type to another
func Map[A, B any](slice []A, f func(A) B) []B {
	r, _ := MapWithErr(slice, func(a A) (B, error) {
		return f(a), nil
	})

	return r
}

// MapWithErr maps a slice of values from one type to another where the mapping can return an error
func MapWithErr[A, B any](slice []A, f func(A) (B, error)) ([]B, error) {
	if slice == nil {
		return nil, nil
	}
	var err error
	result := make([]B, len(slice))
	for i, val := range slice {
		result[i], err = f(val)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
