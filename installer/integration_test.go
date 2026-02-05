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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-1"), deps, nil, nil, pgwf.WorkerID("producer"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-expired"), deps, nil, nil, pgwf.WorkerID("producer"), "", time.Now().Add(-time.Hour)); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-with-payload"), deps, payload, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-resched"), deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("job-extend"), deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("adhoc-job"), deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("unheld-resched"), deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, pgwf.JobID("await-job"), deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, map[string]any{"test": "data"}, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, payload, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// Submit job with different capability
		deps2 := pgwf.JobDependencies{NextNeed: pgwf.Capability("other-cap")}
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "other-job", deps2, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-ready", deps1, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit ready job: %v", err)
		}

		// Active job (leased)
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-active", deps1, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
			t.Fatalf("submit active job: %v", err)
		}
		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"list-test"}, nil)
		if err != nil {
			t.Fatalf("lease job: %v", err)
		}
		if lease.JobID() != "list-active" {
			// Try to get the right job
			_ = lease.Complete(ctx, db)
			if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-active-2", deps1, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit active job 2: %v", err)
			}
			lease, err = pgwf.GetWork(ctx, db, pgwf.WorkerID("worker"), []pgwf.Capability{"list-test"}, nil)
			if err != nil {
				t.Fatalf("lease job 2: %v", err)
			}
		}

		// Completed job
		if err := pgwf.SubmitJob(ctx, db, testTenantID, "list-completed", deps1, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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
		if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
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

// Integration tests for new cursor-based pagination, multi-tenant, and multi-pattern features

func TestListJobs_CursorPagination(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit 10 jobs
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("cursor-test")}
		for i := 0; i < 10; i++ {
			jobID := pgwf.JobID(fmt.Sprintf("cursor-job-%02d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
			time.Sleep(time.Millisecond) // Ensure different timestamps
		}

		// Page 1: Get first 3 jobs
		result1, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Limit:     3,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err != nil {
			t.Fatalf("list page 1: %v", err)
		}
		if len(result1.Jobs) != 3 {
			t.Errorf("expected 3 jobs on page 1, got %d", len(result1.Jobs))
		}
		if !result1.HasMore {
			t.Error("expected HasMore=true on page 1")
		}
		if result1.NextCursor == "" {
			t.Error("expected non-empty NextCursor on page 1")
		}

		// Page 2: Use cursor to get next 3 jobs
		result2, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Cursor:    result1.NextCursor,
			Limit:     3,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err != nil {
			t.Fatalf("list page 2: %v", err)
		}
		if len(result2.Jobs) != 3 {
			t.Errorf("expected 3 jobs on page 2, got %d", len(result2.Jobs))
		}
		if !result2.HasMore {
			t.Error("expected HasMore=true on page 2")
		}

		// Verify no duplicates between pages
		page1IDs := make(map[string]bool)
		for _, job := range result1.Jobs {
			page1IDs[job.JobID] = true
		}
		for _, job := range result2.Jobs {
			if page1IDs[job.JobID] {
				t.Errorf("duplicate job %s found across pages", job.JobID)
			}
		}

		// Page 3: Get remaining jobs
		result3, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Cursor:    result2.NextCursor,
			Limit:     3,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err != nil {
			t.Fatalf("list page 3: %v", err)
		}
		if len(result3.Jobs) != 3 {
			t.Errorf("expected 3 jobs on page 3, got %d", len(result3.Jobs))
		}
		if !result3.HasMore {
			t.Error("expected HasMore=true on page 3")
		}

		// Page 4: Final page
		result4, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Cursor:    result3.NextCursor,
			Limit:     3,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err != nil {
			t.Fatalf("list page 4: %v", err)
		}
		if len(result4.Jobs) != 1 {
			t.Errorf("expected 1 job on page 4, got %d", len(result4.Jobs))
		}
		if result4.HasMore {
			t.Error("expected HasMore=false on final page")
		}
		if result4.NextCursor != "" {
			t.Error("expected empty NextCursor on final page")
		}

		// Verify total unique jobs across all pages
		allJobIDs := make(map[string]bool)
		for _, job := range result1.Jobs {
			allJobIDs[job.JobID] = true
		}
		for _, job := range result2.Jobs {
			allJobIDs[job.JobID] = true
		}
		for _, job := range result3.Jobs {
			allJobIDs[job.JobID] = true
		}
		for _, job := range result4.Jobs {
			allJobIDs[job.JobID] = true
		}
		if len(allJobIDs) != 10 {
			t.Errorf("expected 10 unique jobs across all pages, got %d", len(allJobIDs))
		}
	})
}

func TestListJobs_CursorInvalidation(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit some jobs
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("cursor-invalid")}
		for i := 0; i < 5; i++ {
			jobID := pgwf.JobID(fmt.Sprintf("invalid-cursor-job-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("submitter"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// Get first page
		result1, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Limit:     2,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err != nil {
			t.Fatalf("list page 1: %v", err)
		}

		// Try to use cursor with different query parameters (should fail)
		_, err = pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			Cursor:    result1.NextCursor,
			Limit:     2,
			SortBy:    pgwf.SortByAvailableAt, // Different sort field!
			SortOrder: pgwf.SortDesc,
		})
		if err == nil {
			t.Error("expected error when using cursor with different sort field")
		}
		if !errors.Is(err, pgwf.ErrInvalidCursor) {
			t.Errorf("expected ErrInvalidCursor, got %v", err)
		}

		// Try with different tenant (should fail)
		_, err = pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  "different-tenant",
			Cursor:    result1.NextCursor,
			Limit:     2,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		})
		if err == nil {
			t.Error("expected error when using cursor with different tenant")
		}
		if !errors.Is(err, pgwf.ErrInvalidCursor) {
			t.Errorf("expected ErrInvalidCursor, got %v", err)
		}
	})
}

func TestListJobs_MultiTenantFiltering(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		tenant1 := pgwf.TenantID("tenant-1")
		tenant2 := pgwf.TenantID("tenant-2")
		tenant3 := pgwf.TenantID("tenant-3")

		// Submit jobs for different tenants
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("multi-tenant-test")}
		for i := 0; i < 3; i++ {
			if err := pgwf.SubmitJob(ctx, db, tenant1, pgwf.JobID(fmt.Sprintf("t1-job-%d", i)), deps, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit tenant1 job: %v", err)
			}
		}
		for i := 0; i < 4; i++ {
			if err := pgwf.SubmitJob(ctx, db, tenant2, pgwf.JobID(fmt.Sprintf("t2-job-%d", i)), deps, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit tenant2 job: %v", err)
			}
		}
		for i := 0; i < 2; i++ {
			if err := pgwf.SubmitJob(ctx, db, tenant3, pgwf.JobID(fmt.Sprintf("t3-job-%d", i)), deps, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit tenant3 job: %v", err)
			}
		}

		// Query single tenant
		result1, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(tenant1),
			Limit:    100,
		})
		if err != nil {
			t.Fatalf("list tenant1: %v", err)
		}
		if len(result1.Jobs) != 3 {
			t.Errorf("expected 3 jobs for tenant1, got %d", len(result1.Jobs))
		}
		for _, job := range result1.Jobs {
			if job.TenantID != string(tenant1) {
				t.Errorf("expected tenant_id %s, got %s", tenant1, job.TenantID)
			}
		}

		// Query multiple tenants using TenantIDs
		result2, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantIDs: []string{string(tenant1), string(tenant2)},
			Limit:     100,
		})
		if err != nil {
			t.Fatalf("list tenant1+2: %v", err)
		}
		if len(result2.Jobs) != 7 {
			t.Errorf("expected 7 jobs for tenant1+2, got %d", len(result2.Jobs))
		}

		// Verify all jobs belong to tenant1 or tenant2
		for _, job := range result2.Jobs {
			if job.TenantID != string(tenant1) && job.TenantID != string(tenant2) {
				t.Errorf("unexpected tenant_id %s", job.TenantID)
			}
		}

		// Query all three tenants
		result3, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantIDs: []string{string(tenant1), string(tenant2), string(tenant3)},
			Limit:     100,
		})
		if err != nil {
			t.Fatalf("list all tenants: %v", err)
		}
		if len(result3.Jobs) != 9 {
			t.Errorf("expected 9 jobs for all tenants, got %d", len(result3.Jobs))
		}
	})
}

func TestListJobs_MultiPatternFiltering(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit jobs with different job types
		jobTypes := []string{
			"workflow1:step1",
			"workflow1:step2",
			"workflow2:step1",
			"workflow2:step2",
			"batch:process",
			"batch:finalize",
			"email:send",
			"other:task",
		}

		for i, jobType := range jobTypes {
			deps := pgwf.JobDependencies{NextNeed: pgwf.Capability(jobType)}
			jobID := pgwf.JobID(fmt.Sprintf("pattern-job-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// Query single pattern
		result1, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:       string(testTenantID),
			JobTypePattern: "workflow1:%",
			Limit:          100,
		})
		if err != nil {
			t.Fatalf("list workflow1: %v", err)
		}
		if len(result1.Jobs) != 2 {
			t.Errorf("expected 2 workflow1 jobs, got %d", len(result1.Jobs))
		}

		// Query multiple patterns using JobTypePatterns
		result2, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(testTenantID),
			JobTypePatterns: []string{
				"workflow1:%",
				"workflow2:%",
			},
			Limit: 100,
		})
		if err != nil {
			t.Fatalf("list workflow1+2: %v", err)
		}
		if len(result2.Jobs) != 4 {
			t.Errorf("expected 4 workflow jobs, got %d", len(result2.Jobs))
		}

		// Query with exact match pattern
		result3, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(testTenantID),
			JobTypePatterns: []string{
				"batch:process",
			},
			Limit: 100,
		})
		if err != nil {
			t.Fatalf("list exact batch:process: %v", err)
		}
		if len(result3.Jobs) != 1 {
			t.Errorf("expected 1 batch:process job, got %d", len(result3.Jobs))
		}

		// Query multiple patterns with exact and wildcard
		result4, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID: string(testTenantID),
			JobTypePatterns: []string{
				"batch:%",
				"email:send",
			},
			Limit: 100,
		})
		if err != nil {
			t.Fatalf("list batch+email: %v", err)
		}
		if len(result4.Jobs) != 3 {
			t.Errorf("expected 3 batch+email jobs, got %d", len(result4.Jobs))
		}
	})
}

func TestListJobs_CursorWithMultiTenantAndMultiPattern(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		tenant1 := pgwf.TenantID("cursor-tenant-1")
		tenant2 := pgwf.TenantID("cursor-tenant-2")

		// Submit jobs with different patterns for both tenants
		for i := 0; i < 5; i++ {
			deps1 := pgwf.JobDependencies{NextNeed: pgwf.Capability("type-a:task")}
			deps2 := pgwf.JobDependencies{NextNeed: pgwf.Capability("type-b:task")}

			if err := pgwf.SubmitJob(ctx, db, tenant1, pgwf.JobID(fmt.Sprintf("t1-a-%d", i)), deps1, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit t1 type-a: %v", err)
			}
			if err := pgwf.SubmitJob(ctx, db, tenant1, pgwf.JobID(fmt.Sprintf("t1-b-%d", i)), deps2, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit t1 type-b: %v", err)
			}
			if err := pgwf.SubmitJob(ctx, db, tenant2, pgwf.JobID(fmt.Sprintf("t2-a-%d", i)), deps1, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit t2 type-a: %v", err)
			}
			time.Sleep(time.Millisecond)
		}

		// Query with multi-tenant, multi-pattern, and pagination
		opts := pgwf.ListJobsOptions{
			TenantIDs: []string{string(tenant1), string(tenant2)},
			JobTypePatterns: []string{
				"type-a:%",
				"type-b:%",
			},
			Limit:     5,
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
		}

		// Collect all jobs across pages
		var allJobs []pgwf.JobListItem
		for {
			result, err := pgwf.ListJobs(ctx, db, opts)
			if err != nil {
				t.Fatalf("list jobs: %v", err)
			}
			allJobs = append(allJobs, result.Jobs...)

			if !result.HasMore {
				break
			}
			opts.Cursor = result.NextCursor
		}

		// Verify we got all 15 jobs (5 type-a + 5 type-b for tenant1, 5 type-a for tenant2)
		if len(allJobs) != 15 {
			t.Errorf("expected 15 jobs total, got %d", len(allJobs))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, job := range allJobs {
			key := job.TenantID + ":" + job.JobID
			if seen[key] {
				t.Errorf("duplicate job found: %s", key)
			}
			seen[key] = true
		}
	})
}

func TestListJobs_SortingConsistency(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		// Submit jobs with varying available_at times
		now := time.Now()
		for i := 0; i < 5; i++ {
			deps := pgwf.JobDependencies{
				NextNeed:    pgwf.Capability("sort-test"),
				AvailableAt: now.Add(time.Duration(i) * time.Minute),
			}
			jobID := pgwf.JobID(fmt.Sprintf("sort-job-%d", i))
			if err := pgwf.SubmitJob(ctx, db, testTenantID, jobID, deps, nil, nil, pgwf.WorkerID("sub"), "", time.Time{}); err != nil {
				t.Fatalf("submit job %d: %v", i, err)
			}
		}

		// Sort by created_at DESC
		result1, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortDesc,
			Limit:     100,
		})
		if err != nil {
			t.Fatalf("list by created_at desc: %v", err)
		}
		// Verify descending order
		for i := 1; i < len(result1.Jobs); i++ {
			if result1.Jobs[i].CreatedAt.After(result1.Jobs[i-1].CreatedAt) {
				t.Error("jobs not in descending created_at order")
			}
		}

		// Sort by created_at ASC
		result2, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: pgwf.SortAsc,
			Limit:     100,
		})
		if err != nil {
			t.Fatalf("list by created_at asc: %v", err)
		}
		// Verify ascending order
		for i := 1; i < len(result2.Jobs); i++ {
			if result2.Jobs[i].CreatedAt.Before(result2.Jobs[i-1].CreatedAt) {
				t.Error("jobs not in ascending created_at order")
			}
		}

		// Sort by available_at DESC
		result3, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
			TenantID:  string(testTenantID),
			SortBy:    pgwf.SortByAvailableAt,
			SortOrder: pgwf.SortDesc,
			Limit:     100,
		})
		if err != nil {
			t.Fatalf("list by available_at desc: %v", err)
		}
		// Verify descending order
		for i := 1; i < len(result3.Jobs); i++ {
			if result3.Jobs[i].AvailableAt.After(result3.Jobs[i-1].AvailableAt) {
				t.Error("jobs not in descending available_at order")
			}
		}
	})
}
