package main

func mapArray[T any, R any](values []T, conv func(t T) R) []R {
	results := make([]R, len(values))
	for i, v := range values {
		results[i] = conv(v)
	}
	return results
}
