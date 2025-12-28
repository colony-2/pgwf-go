package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"

	pgwfinstaller "github.com/colony-2/pgwf-go/installer"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

const testTenantID = pgwf.TenantID("test-tenant")

func TestSubmitGetComplete(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("ingest")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-1"), deps, nil, pgwf.WorkerID("producer"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-a"), []pgwf.Capability{"ingest"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease")
		}

		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete: %v", err)
		}
		if err := lease.Complete(ctx, db); !errors.Is(err, pgwf.ErrLeaseExpired) {
			t.Fatalf("expected ErrLeaseExpired on double complete, got %v", err)
		}
	})
}

func TestSubmitWithExpiry(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{
			NextNeed: pgwf.Capability("expiring"),
		}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-expired"), deps, nil, pgwf.WorkerID("producer"), "", time.Now().Add(-time.Hour)); err != nil {
			t.Fatalf("submit with expiry: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-expire"), []pgwf.Capability{"expiring"}, nil)
		if err != nil {
			t.Fatalf("get work for expired job: %v", err)
		}
		if lease != nil {
			t.Fatalf("expected expired job to be unleaseable")
		}
	})
}

func TestSubmitWithPayload(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("payload")}
		payload := map[string]any{"hello": "world", "n": 3}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-with-payload"), deps, payload, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit with payload: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("payload-worker"), []pgwf.Capability{"payload"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease with payload")
		}
		var got map[string]any
		if err := json.Unmarshal(lease.Payload(), &got); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if got["hello"] != "world" || got["n"] != float64(3) {
			t.Fatalf("payload mismatch: %v", got)
		}
		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete: %v", err)
		}
	})
}

func TestRescheduleFlow(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("step1")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-resched"), deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-step1"), []pgwf.Capability{"step1"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected first lease")
		}

		newDeps := pgwf.JobDependencies{NextNeed: pgwf.Capability("step2"), AvailableAt: time.Now()}
		newPayload := map[string]any{"stage": "step2", "attempts": 1}
		if err := lease.Reschedule(ctx, db, newDeps, newPayload); err != nil {
			t.Fatalf("reschedule: %v", err)
		}

		lease2, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-step2"), []pgwf.Capability{"step2"}, nil)
		if err != nil {
			t.Fatalf("get work step2: %v", err)
		}
		if lease2 == nil {
			t.Fatalf("expected rescheduled lease")
		}
		var got map[string]any
		if err := json.Unmarshal(lease2.Payload(), &got); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if got["stage"] != "step2" || got["attempts"] != float64(1) {
			t.Fatalf("unexpected payload after reschedule: %v", got)
		}

		if err := lease2.Complete(ctx, db); err != nil {
			t.Fatalf("complete: %v", err)
		}
	})
}

func TestLeaseExtend(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("extend")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-extend"), deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-extend"), []pgwf.Capability{"extend"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease")
		}

		before := lease.LeaseExpiry()
		if err := lease.Extend(ctx, db, 30*time.Second); err != nil {
			t.Fatalf("extend: %v", err)
		}
		if !lease.LeaseExpiry().After(before) {
			t.Fatalf("expected lease expiry to increase")
		}

		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete: %v", err)
		}
	})
}

func TestCompleteUnheldJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("adhoc")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("adhoc-job"), deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		if err := pgwf.CompleteUnheldJob(ctx, db, testTenantID, pgwf.JobID("adhoc-job"), pgwf.WorkerID("maintainer")); err != nil {
			t.Fatalf("complete unheld: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-adhoc"), []pgwf.Capability{"adhoc"}, nil)
		if err != nil {
			t.Fatalf("get work after unheld complete: %v", err)
		}
		if lease != nil {
			t.Fatalf("expected no work after unheld completion")
		}
	})
}

func TestRescheduleUnheldJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("initial")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("unheld-resched"), deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		newDeps := pgwf.JobDependencies{
			NextNeed:    pgwf.Capability("rescheduled"),
			AvailableAt: time.Now(),
		}
		payload := map[string]any{"phase": "rescheduled"}
		if err := pgwf.RescheduleUnheldJob(ctx, db, testTenantID, pgwf.JobID("unheld-resched"), pgwf.WorkerID("scheduler"), newDeps, payload); err != nil {
			t.Fatalf("reschedule unheld: %v", err)
		}

		leaseOld, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-old"), []pgwf.Capability{"initial"}, nil)
		if err != nil {
			t.Fatalf("get work initial: %v", err)
		}
		if leaseOld != nil {
			t.Fatalf("expected job not to be leased under initial capability")
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-new"), []pgwf.Capability{"rescheduled"}, nil)
		if err != nil {
			t.Fatalf("get work rescheduled: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease after unheld reschedule")
		}
		if lease.JobID() != pgwf.JobID("unheld-resched") {
			t.Fatalf("unexpected job id %s", lease.JobID())
		}
		var got map[string]any
		if err := json.Unmarshal(lease.Payload(), &got); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if got["phase"] != "rescheduled" {
			t.Fatalf("unexpected payload after unheld reschedule: %v", got)
		}
		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete rescheduled lease: %v", err)
		}
	})
}

func TestAwaitWork(t *testing.T) {
	runDatabaseTest(t, func(parent context.Context, db *sql.DB) {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			lease, err := pgwf.AwaitWork(ctx, db, pgwf.WorkerID("await-worker"), []pgwf.Capability{"await"}, nil)
			if err != nil {
				done <- err
				return
			}
			if lease == nil {
				done <- fmt.Errorf("expected lease")
				return
			}
			done <- lease.Complete(ctx, db)
		}()

		time.Sleep(500 * time.Millisecond)
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("await")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("await-job"), deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit: %v", err)
		}

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("await work handler: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("await worker timed out: %v", ctx.Err())
		}
	})
}

func TestInstallerCustomSchema(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		installer := pgwfinstaller.Installer{DB: db, Schema: "custompgwf"}
		if err := installer.Apply(ctx); err != nil {
			t.Fatalf("apply custom: %v", err)
		}
		if err := installer.Verify(ctx); err != nil {
			t.Fatalf("verify custom: %v", err)
		}
		const stmt = `SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'custompgwf' AND table_name = 'jobs'
        )`
		var exists bool
		if err := db.QueryRowContext(ctx, stmt).Scan(&exists); err != nil {
			t.Fatalf("check custom schema: %v", err)
		}
		if !exists {
			t.Fatalf("expected custom schema tables")
		}
	})
}

func TestInstallerVerifyFailsBeforeApply(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		installer := pgwfinstaller.Installer{DB: db, Schema: "missing_schema"}
		if err := installer.Verify(ctx); err == nil {
			t.Fatalf("expected verify error for missing schema")
		}
	})
}

func runDatabaseTest(t *testing.T, fn func(context.Context, *sql.DB)) {
	t.Helper()
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		installer := pgwfinstaller.Installer{DB: db}
		if err := installer.Apply(ctx); err != nil {
			t.Fatalf("apply installer: %v", err)
		}
		if err := installer.Verify(ctx); err != nil {
			t.Fatalf("verify installer: %v", err)
		}
		fn(ctx, db)
	})
}

func withBareDatabase(t *testing.T, fn func(context.Context, *sql.DB)) {
	t.Helper()
	port := uint32(6000 + rand.Intn(1000))
	tempDir := t.TempDir()
	runtimeDir := filepath.Join(tempDir, "runtime")
	dataDir := filepath.Join(runtimeDir, "data")
	cfg := embeddedpostgres.DefaultConfig().
		Port(port).
		RuntimePath(runtimeDir).
		DataPath(dataDir)
	pg := embeddedpostgres.NewDatabase(cfg)
	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = pg.Stop()
	})

	dsn := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", port)
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	fn(ctx, sqlDB)
}

// Query API Integration Tests

func TestGetJobStatus_ActiveJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("process")}
		jobID := pgwf.JobID("status-test-active")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, map[string]any{"test": "data"}, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Get status - should be READY
		status, err := pgwf.GetJobStatus(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("get job status: %v", err)
		}
		if status.Status != pgwf.JobStatusReady {
			t.Errorf("expected READY status, got %s", status.Status)
		}
		if status.JobID != string(jobID) {
			t.Errorf("expected job_id %s, got %s", jobID, status.JobID)
		}
		if status.TenantID != string(testTenantID) {
			t.Errorf("expected tenant_id %s, got %s", testTenantID, status.TenantID)
		}
		if status.NextNeed != "process" {
			t.Errorf("expected next_need 'process', got %s", status.NextNeed)
		}
		if status.ArchivedAt != nil {
			t.Errorf("expected nil archived_at for active job")
		}

		// Lease the job
		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"process"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease")
		}

		// Get status again - should be ACTIVE
		status, err = pgwf.GetJobStatus(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("get job status after lease: %v", err)
		}
		if status.Status != pgwf.JobStatusActive {
			t.Errorf("expected ACTIVE status after lease, got %s", status.Status)
		}
		if status.LeaseID == nil {
			t.Errorf("expected lease_id to be populated")
		}

		// Complete the job
		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete job: %v", err)
		}
	})
}

func TestGetJobStatus_ArchivedJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit and complete a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("archive-test")}
		jobID := pgwf.JobID("status-test-archived")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"archive-test"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete job: %v", err)
		}

		// Get status - should find archived job
		status, err := pgwf.GetJobStatus(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("get job status: %v", err)
		}
		if status.ArchivedAt == nil {
			t.Errorf("expected archived_at to be populated")
		}
		if status.CancelRequested {
			t.Errorf("expected cancel_requested to be false")
		}
	})
}

func TestGetJobStatus_NotFound(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgwf.GetJobStatus(ctx, db, testTenantID, "nonexistent-job")
		if !errors.Is(err, pgwf.ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})
}

func TestCheckJobExists(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("exists-test")}
		jobID := pgwf.JobID("exists-test-job")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Check exists
		exists, err := pgwf.CheckJobExists(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("check job exists: %v", err)
		}
		if !exists.Exists {
			t.Errorf("expected job to exist")
		}
		if exists.JobID != string(jobID) {
			t.Errorf("expected job_id %s, got %s", jobID, exists.JobID)
		}

		// Check nonexistent job
		exists, err = pgwf.CheckJobExists(ctx, db, testTenantID, "nonexistent")
		if err != nil {
			t.Fatalf("check nonexistent job: %v", err)
		}
		if exists.Exists {
			t.Errorf("expected job not to exist")
		}
	})
}

func TestCheckJobExistsWithTenant(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("tenant-test")}
		jobID := pgwf.JobID("tenant-test-job")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Check with correct tenant
		exists, err := pgwf.CheckJobExistsWithTenant(ctx, db, jobID, testTenantID)
		if err != nil {
			t.Fatalf("check with correct tenant: %v", err)
		}
		if !exists.Exists {
			t.Errorf("expected job to exist")
		}

		// Check with wrong tenant
		_, err = pgwf.CheckJobExistsWithTenant(ctx, db, jobID, "wrong-tenant")
		if !errors.Is(err, pgwf.ErrTenantMismatch) {
			t.Errorf("expected ErrTenantMismatch, got %v", err)
		}

		// Check nonexistent job
		_, err = pgwf.CheckJobExistsWithTenant(ctx, db, "nonexistent", testTenantID)
		if !errors.Is(err, pgwf.ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})
}

func TestGetJob_WithAndWithoutPayload(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit a job with payload
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("payload-test")}
		jobID := pgwf.JobID("getjob-payload-test")
		payload := map[string]any{"key": "value", "number": 42}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, payload, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Get without payload
		job, err := pgwf.GetJob(ctx, db, testTenantID, jobID, pgwf.GetJobOptions{IncludePayload: false})
		if err != nil {
			t.Fatalf("get job without payload: %v", err)
		}
		if job.Payload != nil {
			t.Errorf("expected nil payload when IncludePayload=false, got %v", job.Payload)
		}
		if job.NextNeed != "payload-test" {
			t.Errorf("expected next_need 'payload-test', got %s", job.NextNeed)
		}

		// Get with payload
		job, err = pgwf.GetJob(ctx, db, testTenantID, jobID, pgwf.GetJobOptions{IncludePayload: true})
		if err != nil {
			t.Fatalf("get job with payload: %v", err)
		}
		if job.Payload == nil {
			t.Errorf("expected payload when IncludePayload=true")
		}
		var gotPayload map[string]any
		if err := json.Unmarshal(job.Payload, &gotPayload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if gotPayload["key"] != "value" || gotPayload["number"] != float64(42) {
			t.Errorf("payload mismatch: %v", gotPayload)
		}
	})
}

func TestFindJobs(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit multiple jobs with same capability
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("find-test")}
		for i := 0; i < 5; i++ {
			jobID := pgwf.JobID(fmt.Sprintf("find-test-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// Submit job with different capability
		deps2 := pgwf.JobDependencies{NextNeed: pgwf.Capability("other-cap")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "other-job", deps2, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit other job: %v", err)
		}

		// Find jobs with specific capability
		jobs, err := pgwf.FindJobs(ctx, db, pgwf.FindJobsOptions{
			TenantIDs: []string{string(testTenantID)},
			Status:    pgwf.JobStatusReady,
			NextNeed:  "find-test",
			Limit:     10,
		})
		if err != nil {
			t.Fatalf("find jobs: %v", err)
		}
		if len(jobs) != 5 {
			t.Errorf("expected 5 jobs, got %d", len(jobs))
		}
		for _, job := range jobs {
			if job.NextNeed != "find-test" {
				t.Errorf("expected next_need 'find-test', got %s", job.NextNeed)
			}
			if job.Status != pgwf.JobStatusReady {
				t.Errorf("expected READY status, got %s", job.Status)
			}
		}
	})
}

func TestListJobs_WithFilters(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit jobs with different states
		// Ready job
		deps1 := pgwf.JobDependencies{NextNeed: pgwf.Capability("list-test")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-ready", deps1, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit ready job: %v", err)
		}

		// Active job (leased)
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-active", deps1, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit active job: %v", err)
		}
		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"list-test"}, nil)
		if err != nil {
			t.Fatalf("lease job: %v", err)
		}
		if lease.JobID() != "list-active" {
			// Try to get the right job
			_ = lease.Complete(ctx, db)
			if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-active-2", deps1, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit active job 2: %v", err)
			}
			lease, err = pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"list-test"}, nil)
			if err != nil {
				t.Fatalf("lease job 2: %v", err)
			}
		}

		// Completed job
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-completed", deps1, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit completed job: %v", err)
		}
		completeLease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker2"), []pgwf.Capability{"list-test"}, nil)
		if err != nil {
			t.Fatalf("lease completed job: %v", err)
		}
		if err := completeLease.Complete(ctx, db); err != nil {
			t.Fatalf("complete job: %v", err)
		}

		// List only READY jobs
		result, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(testTenantID),
			Statuses: []pgwf.JobStatus{pgwf.JobStatusReady},
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("list ready jobs: %v", err)
		}
		readyCount := 0
		for _, job := range result.Jobs {
			if job.Status == pgwf.JobStatusReady {
				readyCount++
			}
		}
		if readyCount == 0 {
			t.Errorf("expected at least one READY job")
		}

		// List with IncludeArchived
		result, err = pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:        string(testTenantID),
			IncludeArchived: true,
			Limit:           10,
		})
		if err != nil {
			t.Fatalf("list with archived: %v", err)
		}
		foundArchived := false
		for _, job := range result.Jobs {
			if job.ArchivedAt != nil {
				foundArchived = true
				break
			}
		}
		if !foundArchived {
			t.Errorf("expected to find archived jobs when IncludeArchived=true")
		}
	})
}

func TestGetJobStatusBatch(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit multiple jobs
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("batch-test")}
		jobIDs := []pgwf.JobID{"batch-1", "batch-2", "batch-3"}
		for _, jobID := range jobIDs {
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %s: %v", jobID, err)
			}
		}

		// Get batch status
		statuses, err := pgwf.GetJobStatusBatch(ctx, db, testTenantID, jobIDs)
		if err != nil {
			t.Fatalf("get batch status: %v", err)
		}
		if len(statuses) != 3 {
			t.Errorf("expected 3 statuses, got %d", len(statuses))
		}
		for _, jobID := range jobIDs {
			status, ok := statuses[string(jobID)]
			if !ok {
				t.Errorf("expected status for job %s", jobID)
				continue
			}
			if status.Status != pgwf.JobStatusReady {
				t.Errorf("expected READY status for %s, got %s", jobID, status.Status)
			}
		}

		// Test with nonexistent jobs mixed in
		mixedIDs := append(jobIDs, "nonexistent-1", "nonexistent-2")
		statuses, err = pgwf.GetJobStatusBatch(ctx, db, testTenantID, mixedIDs)
		if err != nil {
			t.Fatalf("get batch with nonexistent: %v", err)
		}
		if len(statuses) != 3 {
			t.Errorf("expected 3 statuses (existing only), got %d", len(statuses))
		}
	})
}

func TestIsJobArchived(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit and complete a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("archived-check")}
		jobID := pgwf.JobID("archived-check-job")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Check before archiving
		archived, err := pgwf.IsJobArchived(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("check archived before: %v", err)
		}
		if archived {
			t.Errorf("expected job not to be archived initially")
		}

		// Complete the job
		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"archived-check"}, nil)
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if err := lease.Complete(ctx, db); err != nil {
			t.Fatalf("complete job: %v", err)
		}

		// Check after archiving
		archived, err = pgwf.IsJobArchived(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("check archived after: %v", err)
		}
		if !archived {
			t.Errorf("expected job to be archived after completion")
		}
	})
}

func TestListArchivedJobs(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit and complete multiple jobs
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("archive-list")}
		for i := 0; i < 3; i++ {
			jobID := pgwf.JobID(fmt.Sprintf("archive-list-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
			lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"archive-list"}, nil)
			if err != nil {
				t.Fatalf("get work %d: %v", i, err)
			}
			if err := lease.Complete(ctx, db); err != nil {
				t.Fatalf("complete job %d: %v", i, err)
			}
		}

		// List archived jobs
		result, err := pgwf.ListArchivedJobs(ctx, db, pgwf.ListArchivedJobsOptions{
			TenantID: string(testTenantID),
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("list archived jobs: %v", err)
		}
		if len(result.Jobs) < 3 {
			t.Errorf("expected at least 3 archived jobs, got %d", len(result.Jobs))
		}
		for _, job := range result.Jobs {
			if job.ArchivedAt == nil {
				t.Errorf("expected archived_at to be populated for job %s", job.JobID)
			}
		}
	})
}

func TestListJobs_Pagination(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit many jobs
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("pagination-test")}
		for i := 0; i < 10; i++ {
			jobID := pgwf.JobID(fmt.Sprintf("page-test-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// List with small limit
		result, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(testTenantID),
			Limit:    3,
		})
		if err != nil {
			t.Fatalf("list jobs: %v", err)
		}
		if len(result.Jobs) != 3 {
			t.Errorf("expected 3 jobs with limit=3, got %d", len(result.Jobs))
		}
		if !result.HasMore {
			t.Errorf("expected HasMore=true when there are more results")
		}
	})
}

func TestGetJobStatus_CancelledJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit a job
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("cancel-test")}
		jobID := pgwf.JobID("cancel-status-test")
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit job: %v", err)
		}

		// Cancel the job
		if err := pgwf.CancelJob(ctx, db, testTenantID, jobID, pgwf.WorkerID("canceller"), "test cancellation"); err != nil {
			t.Fatalf("cancel job: %v", err)
		}

		// Get status
		status, err := pgwf.GetJobStatus(ctx, db, testTenantID, jobID)
		if err != nil {
			t.Fatalf("get job status: %v", err)
		}
		if status.Status != pgwf.JobStatusCancelled {
			t.Errorf("expected CANCELLED status, got %s", status.Status)
		}
		if !status.CancelRequested {
			t.Errorf("expected cancel_requested to be true")
		}
		if status.CancelRequestedBy == nil || *status.CancelRequestedBy != "canceller" {
			t.Errorf("expected cancel_requested_by to be 'canceller'")
		}
	})
}
