package pgwf

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type stubDB struct{}

func (stubDB) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return new(sql.Row)
}

func TestSubmitJobValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deps := JobDependencies{NextNeed: Capability("cap")}
	cases := []struct {
		name   string
		ctx    context.Context
		db     DB
		jobID  JobID
		worker WorkerID
		deps   JobDependencies
	}{
		{name: "nil db", ctx: ctx, db: nil, jobID: "job", worker: "w", deps: deps},
		{name: "nil ctx", ctx: nil, db: stubDB{}, jobID: "job", worker: "w", deps: deps},
		{name: "empty job", ctx: ctx, db: stubDB{}, jobID: "", worker: "w", deps: deps},
		{name: "empty worker", ctx: ctx, db: stubDB{}, jobID: "job", worker: "", deps: deps},
		{name: "missing capability", ctx: ctx, db: stubDB{}, jobID: "job", worker: "w", deps: JobDependencies{}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := SubmitJob(tc.ctx, tc.db, tc.jobID, tc.deps, tc.worker); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestGetWorkValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name   string
		ctx    context.Context
		db     DB
		worker WorkerID
		caps   []Capability
	}{
		{name: "nil db", ctx: ctx, db: nil, worker: "w", caps: []Capability{"cap"}},
		{name: "nil ctx", ctx: nil, db: stubDB{}, worker: "w", caps: []Capability{"cap"}},
		{name: "empty worker", ctx: ctx, db: stubDB{}, worker: "", caps: []Capability{"cap"}},
		{name: "no caps", ctx: ctx, db: stubDB{}, worker: "w", caps: nil},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GetWork(tc.ctx, tc.db, tc.worker, tc.caps); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestAwaitWorkValidation(t *testing.T) {
	t.Parallel()
	if _, err := AwaitWork(nil, stubDB{}, WorkerID("w"), []Capability{"cap"}); err == nil {
		t.Fatalf("expected nil context error")
	}
}

func TestLeaseExtendValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lease := &Lease{jobID: "job", leaseID: "lease", worker: "worker", leaseExpires: time.Now().Add(time.Minute)}
	if err := lease.Extend(ctx, nil, time.Second); err == nil {
		t.Fatalf("expected nil db error")
	}
	if err := lease.Extend(ctx, stubDB{}, 0); err == nil {
		t.Fatalf("expected duration error")
	}
}

func TestLeaseRescheduleValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lease := &Lease{jobID: "job", leaseID: "lease", worker: "worker", leaseExpires: time.Now().Add(time.Minute)}
	if err := lease.Reschedule(ctx, stubDB{}, JobDependencies{}); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestLeaseValidateActiveFailures(t *testing.T) {
	t.Parallel()
	if err := (*Lease)(nil).validateActive(); err == nil {
		t.Fatalf("expected error for nil lease")
	}
	lease := &Lease{leaseExpires: time.Now().Add(-time.Second)}
	if err := lease.validateActive(); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestLeaseAccessorsNil(t *testing.T) {
	t.Parallel()
	var lease *Lease
	if lease.JobID() != "" || lease.LeaseID() != "" {
		t.Fatalf("expected empty values for nil lease")
	}
	if !lease.LeaseExpiry().IsZero() {
		t.Fatalf("expected zero time for nil lease")
	}
}

func TestSecondsForExtension(t *testing.T) {
	t.Parallel()
	lease := &Lease{leaseExpires: time.Now().Add(-time.Second)}
	if got := lease.secondsForExtension(500 * time.Millisecond); got < 1 {
		t.Fatalf("seconds should round up, got %d", got)
	}
	lease2 := &Lease{leaseExpires: time.Now().Add(2 * time.Second)}
	if got := lease2.secondsForExtension(0); got != 3 {
		t.Fatalf("expected rounded seconds to include minimum additional second, got %d", got)
	}
}

func TestSentinelHelpers(t *testing.T) {
	t.Parallel()
	base := errors.New("base")
	marker := errors.New("marker")
	wrapped := wrap(marker, base)
	if !errors.Is(wrapped, marker) || !errors.Is(wrapped, base) {
		t.Fatalf("wrap should retain marker and base")
	}
	if wrap(marker, nil) != marker {
		t.Fatalf("wrap should return marker when err nil")
	}
	if wrap(nil, base) != base {
		t.Fatalf("wrap should return err when marker nil")
	}
	se := sentinelError{marker: marker}
	if se.Error() == "" {
		t.Fatalf("expected marker error text")
	}
	if se.Unwrap() != nil {
		t.Fatalf("expected nil cause")
	}
	if annotateError(nil) != nil {
		t.Fatalf("nil passthrough")
	}
	tests := []struct {
		msg  string
		want error
	}{
		{msg: "references unknown jobs", want: ErrDependencyViolation},
		{msg: "not currently leased", want: ErrLeaseMismatch},
		{msg: "has expired", want: ErrLeaseExpired},
		{msg: "object not found", want: ErrJobNotFound},
	}
	for _, tc := range tests {
		if err := annotateError(errors.New(tc.msg)); !errors.Is(err, tc.want) {
			t.Fatalf("expected %v for %q, got %v", tc.want, tc.msg, err)
		}
	}
}
