package installer_test

import (
	"context"
	"database/sql"
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

func TestSubmitGetComplete(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("ingest")}
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("job-1"), deps, pgwf.WorkerID("producer")); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-a"), []pgwf.Capability{"ingest"})
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

func TestRescheduleFlow(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("step1")}
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("job-resched"), deps, pgwf.WorkerID("submitter")); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-step1"), []pgwf.Capability{"step1"})
		if err != nil {
			t.Fatalf("get work: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected first lease")
		}

		newDeps := pgwf.JobDependencies{NextNeed: pgwf.Capability("step2"), AvailableAt: time.Now()}
		if err := lease.Reschedule(ctx, db, newDeps); err != nil {
			t.Fatalf("reschedule: %v", err)
		}

		lease2, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-step2"), []pgwf.Capability{"step2"})
		if err != nil {
			t.Fatalf("get work step2: %v", err)
		}
		if lease2 == nil {
			t.Fatalf("expected rescheduled lease")
		}

		if err := lease2.Complete(ctx, db); err != nil {
			t.Fatalf("complete: %v", err)
		}
	})
}

func TestLeaseExtend(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("extend")}
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("job-extend"), deps, pgwf.WorkerID("submitter")); err != nil {
			t.Fatalf("submit: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-extend"), []pgwf.Capability{"extend"})
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
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("adhoc-job"), deps, pgwf.WorkerID("submitter")); err != nil {
			t.Fatalf("submit: %v", err)
		}

		if err := pgwf.CompleteUnheldJob(ctx, db, pgwf.JobID("adhoc-job"), pgwf.WorkerID("maintainer")); err != nil {
			t.Fatalf("complete unheld: %v", err)
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-adhoc"), []pgwf.Capability{"adhoc"})
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
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("unheld-resched"), deps, pgwf.WorkerID("submitter")); err != nil {
			t.Fatalf("submit: %v", err)
		}

		newDeps := pgwf.JobDependencies{
			NextNeed:    pgwf.Capability("rescheduled"),
			AvailableAt: time.Now(),
		}
		if err := pgwf.RescheduleUnheldJob(ctx, db, pgwf.JobID("unheld-resched"), pgwf.WorkerID("scheduler"), newDeps); err != nil {
			t.Fatalf("reschedule unheld: %v", err)
		}

		leaseOld, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-old"), []pgwf.Capability{"initial"})
		if err != nil {
			t.Fatalf("get work initial: %v", err)
		}
		if leaseOld != nil {
			t.Fatalf("expected job not to be leased under initial capability")
		}

		lease, err := pgwf.GetWork(ctx, db, pgwf.WorkerID("worker-new"), []pgwf.Capability{"rescheduled"})
		if err != nil {
			t.Fatalf("get work rescheduled: %v", err)
		}
		if lease == nil {
			t.Fatalf("expected lease after unheld reschedule")
		}
		if lease.JobID() != pgwf.JobID("unheld-resched") {
			t.Fatalf("unexpected job id %s", lease.JobID())
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
			lease, err := pgwf.AwaitWork(ctx, db, pgwf.WorkerID("await-worker"), []pgwf.Capability{"await"})
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
		if err := pgwf.SubmitJob(ctx, db, pgwf.JobID("await-job"), deps, pgwf.WorkerID("submitter")); err != nil {
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
