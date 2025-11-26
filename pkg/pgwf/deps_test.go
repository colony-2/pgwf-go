package pgwf

import "testing"

func TestSingletonArg(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		want any
	}{
		{name: "zero value", key: "", want: nil},
		{name: "empty", key: "", want: nil},
		{name: "value", key: "foo", want: "foo"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := singletonArg(tc.key); got != tc.want {
				t.Fatalf("singleton arg mismatch: got %v want %v", got, tc.want)
			}
		})
	}
}
