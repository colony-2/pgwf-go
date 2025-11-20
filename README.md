# pgwf-go

`pgwf-go` is a small Go companion for the [`pgwf`](https://github.com/colony-2/pgwf) PostgreSQL workflow engine. It exposes a small set of helpers for inserting workflow metadata, leasing jobs, and safely rescheduling/completing work without forcing you to write SQL by hand. Everything is built directly on top of `database/sql`, so you can pass either `*sql.DB` or `*sql.Tx` wherever a `pgwf.DB` interface is accepted.

## Packages

- `pkg/pgwf` – core helpers used by producers and workers. They map 1:1 to the canonical SQL functions (`pgwf.submit_job`, `pgwf.get_work`, `pgwf.extend_lease`, etc.), add input validation, and provide local lease safety checks.
- `installer` – optional module that embeds the upstream SQL, making it easy to apply or verify the schema from Go. Import it only when you need to bootstrap a database; the main package stays lean and does not ship the SQL blob.

## API overview

### Submission

```go
func SubmitJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, deps pgwf.JobDependencies, worker pgwf.WorkerID) error
```

- Validates non-empty IDs and required dependency fields.
- Accepts `*sql.DB` or `*sql.Tx`, enabling atomic submission alongside your own business tables.
- Wraps dependency/singleton violations in `pgwf.ErrDependencyViolation`.

### Polling for work

```go
func GetWork(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability) (*pgwf.Lease, error)
func AwaitWork(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability) (*pgwf.Lease, error)
```

- `GetWork` is a single call to `pgwf.get_work` (limit 1, 60‑second lease).
- `AwaitWork` wraps `GetWork` in an exponential backoff loop until the context is done or a lease is returned.

### Lease helpers

```go
func (l *pgwf.Lease) WithKeepAlive(db *sql.DB) *pgwf.Lease
func (l *pgwf.Lease) Extend(ctx context.Context, db pgwf.DB, additional time.Duration) error
func (l *pgwf.Lease) Reschedule(ctx context.Context, db pgwf.DB, deps pgwf.JobDependencies) error
func (l *pgwf.Lease) Complete(ctx context.Context, db pgwf.DB) error
```

- Each method verifies the lease is present, unreleased, and unexpired before reaching the database and returns `pgwf.ErrLeaseExpired` immediately if not.
- `WithKeepAlive` spins up an internal goroutine (using a real `*sql.DB`) that refreshes the lease until you complete or reschedule it.
- Error helpers wrap driver errors in sentinel `pgwf.ErrLeaseMismatch`, `pgwf.ErrJobNotFound`, or `pgwf.ErrDependencyViolation` values for easier inspection.

### Unheld job helpers

```go
func pgwf.CompleteUnheldJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID) error
func pgwf.RescheduleUnheldJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID, deps pgwf.JobDependencies) error
```

- Mirror the lease-based APIs but operate directly on ready jobs by ID (no lease required).
- Require the caller to declare the acting worker for trace/log context and reuse the same dependency validation rules as `Lease.Reschedule`.

### Installer module

```go
type Installer struct {
    DB     *sql.DB
    Schema string // optional, defaults to "pgwf"
}

func (Installer) Apply(ctx context.Context) error
func (Installer) Verify(ctx context.Context) error
```

Use it to bootstrap or check a database. It replaces the default schema name if `Schema` is provided, so you can run multiple pgwf copies side-by-side.

## Example usage

The snippet below shows a simple transaction that writes a payload alongside workflow metadata, followed by a worker loop that leases, processes, and reschedules jobs as needed.

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/colony-2/pgwf-go/pkg/pgwf"
)

func enqueueEmail(ctx context.Context, db *sql.DB, emailID string) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    if _, err := tx.ExecContext(ctx, `INSERT INTO emails(id, status) VALUES($1, 'pending')`, emailID); err != nil {
        return err
    }
    deps := pgwf.JobDependencies{
        NextNeed:    pgwf.Capability("send_email"),
        WaitFor:     nil,
        SingletonKey: emailID, // prevent duplicates
    }
    if err := pgwf.SubmitJob(ctx, tx, pgwf.JobID(emailID), deps, pgwf.WorkerID("api")); err != nil {
        return err
    }
    return tx.Commit()
}

func workerLoop(ctx context.Context, db *sql.DB) {
    workerID := pgwf.WorkerID("mailer-1")
    caps := []pgwf.Capability{"send_email"}

    for ctx.Err() == nil {
        lease, err := pgwf.AwaitWork(ctx, db, workerID, caps)
        if err != nil {
            log.Printf("fetch failed: %v", err)
            continue
        }
        if lease == nil {
            continue
        }
        lease.WithKeepAlive(db)

        if err := handleEmail(ctx, lease.JobID()); err != nil {
            // back off for 5 minutes before retrying
            deps := pgwf.JobDependencies{
                NextNeed:    pgwf.Capability("send_email"),
                AvailableAt: time.Now().Add(5 * time.Minute),
            }
            if rerr := lease.Reschedule(ctx, db, deps); rerr != nil {
                log.Printf("reschedule failed: %v", rerr)
            }
            continue
        }
        if err := lease.Complete(ctx, db); err != nil {
            log.Printf("complete failed: %v", err)
        }
    }
}

func handleEmail(ctx context.Context, jobID pgwf.JobID) error {
    // send the email identified by jobID...
    return nil
}
```

## Getting started

1. Apply or verify the schema (optional) by importing `github.com/colony-2/pgwf-go/installer` and calling `Installer.Apply` / `Installer.Verify`.
2. Wire the `pkg/pgwf` helpers into your producer and worker processes.
3. Watch for sentinel errors (`ErrLeaseExpired`, `ErrLeaseMismatch`, `ErrJobNotFound`, `ErrDependencyViolation`) to decide whether to retry, drop, or resubmit work.
