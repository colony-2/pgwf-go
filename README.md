# pgwf-go

`pgwf-go` is a small Go companion for the [`pgwf`](https://github.com/colony-2/pgwf) PostgreSQL workflow engine. It exposes a small set of helpers for inserting workflow metadata, leasing jobs, and safely rescheduling/completing work without forcing you to write SQL by hand. Everything is built directly on top of `database/sql`, so you can pass either `*sql.DB` or `*sql.Tx` wherever a `pgwf.DB` interface is accepted.

## Packages

- `pkg/pgwf` – core helpers used by producers and workers. They map 1:1 to the canonical SQL functions (`pgwf.submit_job`, `pgwf.get_work`, `pgwf.extend_lease`, etc.), add input validation, and provide local lease safety checks.
- `installer` – optional module that embeds the upstream SQL, making it easy to apply or verify the schema from Go. Import it only when you need to bootstrap a database; the main package stays lean and does not ship the SQL blob.

## API overview

### Query APIs

pgwf-go provides a comprehensive set of read-only query APIs that eliminate the need to directly access underlying database tables.

#### Job Status Query

Check the status and metadata of a specific job without leasing it:

```go
func GetJobStatus(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID) (*pgwf.JobStatusInfo, error)
```

Returns detailed job information including status (READY, ACTIVE, CANCELLED, etc.), lease information, dependencies, and timing. Searches both active and archived jobs.

#### Job Existence Check

Verify if a job exists and optionally validate tenant ownership:

```go
func CheckJobExists(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID) (*pgwf.JobExistence, error)
func CheckJobExistsWithTenant(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, expectedTenantID pgwf.TenantID) (*pgwf.JobExistence, error)
```

Useful for idempotency checks before submitting duplicate jobs.

#### Get Job by ID

Retrieve full job information without leasing:

```go
func GetJob(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID, opts pgwf.GetJobOptions) (*pgwf.JobDetail, error)
```

Set `opts.IncludePayload = true` to include the job payload (excluded by default for efficiency).

#### Find Jobs by Criteria

Find jobs matching specific criteria (useful for external task workers):

```go
func FindJobs(ctx context.Context, db pgwf.DB, opts pgwf.FindJobsOptions) ([]pgwf.JobInfo, error)
```

Example - find all READY jobs waiting for a specific capability across multiple tenants:

```go
jobs, err := pgwf.FindJobs(ctx, db, pgwf.FindJobsOptions{
    TenantIDs: []string{"tenant-1", "tenant-2"},
    Status:    pgwf.JobStatusReady,
    NextNeed:  "workflow:emailTask",
    Limit:     100,
})
```

#### List Jobs with Filtering and Pagination

Query jobs with advanced filtering and cursor-based pagination:

```go
func ListJobs(ctx context.Context, db pgwf.DB, opts pgwf.ListJobsOptions) (*pgwf.ListJobsResult, error)
```

**Features**:
- **Multi-tenant filtering**: Query multiple tenants in a single call using `TenantIDs []string`
- **Multi-pattern job type filtering**: Filter by multiple LIKE patterns using `JobTypePatterns []string` (OR semantics)
- **Metadata filtering**: Filter by JSON metadata fields using `MetadataEquals` (IN semantics per path)
- **Cursor-based pagination**: Stateless pagination using opaque cursor tokens
- **Status filtering**: Filter by multiple job statuses
- **Completion status filtering**: Filter archived jobs by completion status (requires `IncludeArchived: true`)
- **Time range filtering**: Filter by creation time
- **Sorting**: Sort by created_at, available_at, or job_id (ASC/DESC)
- **Archive support**: Include archived jobs with `IncludeArchived: true`

Example - paginated list with multiple filters:

```go
opts := pgwf.ListJobsOptions{
    TenantIDs: []string{"tenant-1", "tenant-2"},  // Multi-tenant
    Statuses:  []pgwf.JobStatus{pgwf.JobStatusReady, pgwf.JobStatusActive},
    JobTypePatterns: []string{"workflow1:%", "workflow2:%", "batch:process"},  // Multi-pattern
    MetadataEquals: []pgwf.MetadataPredicate{
        {Path: []string{"source"}, Values: []any{"api", "scheduler"}},
    },
    Limit:     50,
    SortBy:    pgwf.SortByCreatedAt,
    SortOrder: pgwf.SortDesc,
}

for {
    result, err := pgwf.ListJobs(ctx, db, opts)
    if err != nil {
        return err
    }

    for _, job := range result.Jobs {
        // Process job
    }

    if !result.HasMore {
        break
    }
    opts.Cursor = result.NextCursor  // Use cursor for next page
}
```

**Cursor Pagination**: Cursors are opaque tokens that encode:
- Last seen value of the sort field
- Last seen job_id (for tie-breaking)
- Query fingerprint (validates cursor matches current query parameters)

Cursors are validated on each request and return `ErrInvalidCursor` if parameters change.

#### Batch Status Query

Check status of multiple jobs efficiently:

```go
func GetJobStatusBatch(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobIDs []pgwf.JobID) (map[string]*pgwf.JobStatusInfo, error)
```

Returns a map of jobID → JobStatusInfo. Jobs that don't exist are omitted from results.

#### Archive Query

Check if a job is archived or query archived jobs specifically:

```go
func IsJobArchived(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID) (bool, error)
func ListArchivedJobs(ctx context.Context, db pgwf.DB, opts pgwf.ListArchivedJobsOptions) (*pgwf.ListJobsResult, error)
```

`ListArchivedJobs` is more efficient than `ListJobs` when you only need completed jobs.

### Submission

```go
func SubmitJob(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID, deps pgwf.JobDependencies, payload any, metadata any, worker pgwf.WorkerID, expiresAt time.Time) error
```

- Validates non-empty IDs and required dependency fields.
- Accepts an immutable JSON payload (object, ≤512 bytes stored) that workers will receive on lease.
- Accepts immutable JSON metadata (object) stored with the job and queryable via list/status APIs.
- Optional `expiresAt` sets `pgwf.jobs.expires_at`; leave zero to keep the job leaseable indefinitely.
- Alternate capability fallback: set `deps.Alternate = &pgwf.AlternateNext{Need: "cap.alt", After: 5 * time.Minute}` to pivot to `cap.alt` once the job has been READY and unleased for 5 minutes. Use `Alternate = &pgwf.AlternateNext{}` in a reschedule to clear any existing alternate.
- Accepts `*sql.DB` or `*sql.Tx`, enabling atomic submission alongside your own business tables.
- Wraps dependency violations in `pgwf.ErrDependencyViolation`.

### Polling for work

```go
func GetWork(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability, tenantIDs []pgwf.TenantID) (*pgwf.Lease, error)
func GetWorkWithOptions(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability, opts pgwf.GetWorkOptions) (*pgwf.Lease, error)
func AwaitWork(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability, tenantIDs []pgwf.TenantID) (*pgwf.Lease, error)
func AwaitWorkWithOptions(ctx context.Context, db pgwf.DB, worker pgwf.WorkerID, capabilities []pgwf.Capability, opts pgwf.GetWorkOptions) (*pgwf.Lease, error)

type GetWorkOptions struct {
    TenantIDs      []pgwf.TenantID
    LeaseSeconds   int
    MetadataEquals []pgwf.MetadataPredicate
}
```

- `GetWork` is the convenience wrapper when you only need a tenant filter; `nil` tenant IDs means all tenants.
- `GetWorkWithOptions` is the full single-shot lease API and supports tenant filtering, custom lease duration, and metadata equality filters.
- `AwaitWork` is the convenience polling wrapper around `GetWork`.
- `AwaitWorkWithOptions` wraps `GetWorkWithOptions` in an exponential backoff loop until the context is done or a lease is returned.
- `Lease.Payload()` returns the job payload as raw JSON (default `{}` if unset).

### Lease a Specific Job

```go
func GetJobLease(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID, worker pgwf.WorkerID, capabilities []pgwf.Capability) (*pgwf.Lease, error)
func GetJobLeaseWithOptions(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID, worker pgwf.WorkerID, capabilities []pgwf.Capability, opts pgwf.GetJobLeaseOptions) (*pgwf.Lease, error)
```

- `GetJobLease` is the direct, single-shot path for leasing one known job ID without going through the broader queue scan/backoff flow.
- It still enforces normal lease rules: the job must be `READY` and match one of the supplied capabilities.
- `GetJobLeaseWithOptions` currently supports `LeaseSeconds` for custom lease durations.

### Lease helpers

```go
func (l *pgwf.Lease) WithKeepAlive(db *sql.DB) *pgwf.Lease
func (l *pgwf.Lease) Extend(ctx context.Context, db pgwf.DB, additional time.Duration) error
func (l *pgwf.Lease) Reschedule(ctx context.Context, db pgwf.DB, deps pgwf.JobDependencies, payload any) error
func (l *pgwf.Lease) Complete(ctx context.Context, db pgwf.DB) error
func (l *pgwf.Lease) CompleteWithStatus(ctx context.Context, db pgwf.DB, status pgwf.CompletionStatus, completionDetail string) error
```

- Each method verifies the lease is present, unreleased, and unexpired before reaching the database and returns `pgwf.ErrLeaseExpired` immediately if not.
- `WithKeepAlive` spins up an internal goroutine (using a real `*sql.DB`) that refreshes the lease until you complete or reschedule it.
- `Reschedule` optionally replaces the payload while updating dependencies; pass `nil` to leave the current payload intact.
- `Complete` defaults to `pgwf.CompletionStatusSucceeded`. Use `CompleteWithStatus` to record an explicit completion status and optional completion detail.
- Error helpers wrap driver errors in sentinel `pgwf.ErrLeaseMismatch`, `pgwf.ErrJobNotFound`, or `pgwf.ErrDependencyViolation` values for easier inspection.

### Unheld job helpers

```go
func pgwf.CompleteUnheldJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID) error
func pgwf.CompleteUnheldJobWithStatus(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID, status pgwf.CompletionStatus, completionDetail string) error
func pgwf.RescheduleUnheldJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID, deps pgwf.JobDependencies, payload any) error
```

- Mirror the lease-based APIs but operate directly on ready jobs by ID (no lease required).
- Require the caller to declare the acting worker for trace/log context and reuse the same dependency validation rules as `Lease.Reschedule`.
- Supply a payload to override the stored JSON object during reschedule; pass `nil` to keep the existing payload.

### Cancellation

```go
func pgwf.CancelJob(ctx context.Context, db pgwf.DB, jobID pgwf.JobID, worker pgwf.WorkerID, reason string) error
```

- Marks queued or active jobs for cancellation via `pgwf.cancel_job`.
- `reason` is optional (pass `""` to record `NULL`) and is recorded in the trace event emitted by pgwf.
- Surfaces the same sentinel errors (`ErrJobNotFound`, `ErrLeaseMismatch`, etc.) as other helpers.

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
        NextNeed: pgwf.Capability("send_email"),
        WaitFor:  nil,
    }
    payload := map[string]any{"email_id": emailID}
    if err := pgwf.SubmitJob(ctx, tx, pgwf.TenantID("default"), pgwf.JobID(emailID), deps, payload, nil, pgwf.WorkerID("api"), time.Time{}); err != nil {
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
            // include the failure reason so the next run can react accordingly
            newPayload := map[string]any{"email_id": emailID, "last_error": err.Error()}
            if rerr := lease.Reschedule(ctx, db, deps, newPayload); rerr != nil {
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

## Sentinel Errors

The query APIs return specific sentinel errors for common failure cases:

```go
var (
    ErrJobNotFound      error // Job doesn't exist in active or archived tables
    ErrTenantMismatch   error // Job exists but belongs to different tenant
    ErrInvalidCursor    error // Pagination cursor is invalid or doesn't match query
    ErrInvalidOptions   error // Query options are invalid
    ErrLeaseExpired     error // Lease has expired (lease APIs)
    ErrLeaseMismatch    error // Lease ID mismatch (lease APIs)
    ErrDependencyViolation error // Dependency constraint violation (submission)
)
```

Use `errors.Is()` to check for these errors and handle appropriately.

## Getting started

1. Apply or verify the schema (optional) by importing `github.com/colony-2/pgwf-go/installer` and calling `Installer.Apply` / `Installer.Verify`.
2. Wire the `pkg/pgwf` helpers into your producer and worker processes.
3. Use query APIs (`GetJobStatus`, `ListJobs`, etc.) to monitor and inspect jobs without directly accessing database tables.
4. Watch for sentinel errors (`ErrLeaseExpired`, `ErrLeaseMismatch`, `ErrJobNotFound`, `ErrDependencyViolation`, `ErrInvalidCursor`) to decide whether to retry, drop, or resubmit work.
