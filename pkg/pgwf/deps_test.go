package pgwf

import "testing"

func TestSingletonArg(t *testing.T) {
	t.Parallel()
	var deps JobDependencies
	if deps.singletonArg() != nil {
		t.Fatalf("zero value singleton should be nil")
	}

	deps.SingletonKey = ""
	if deps.singletonArg() != nil {
		t.Fatalf("empty singleton should be nil")
	}

	deps.SingletonKey = "foo"
	got := deps.singletonArg()
	key, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if key != "foo" {
		t.Fatalf("singleton arg mismatch: got %q", key)
	}
}
