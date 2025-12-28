package pgwf

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// JobStatus represents the possible states returned by pgwf.jobs_with_status view
type JobStatus string

const (
	JobStatusActive         JobStatus = "ACTIVE"          // Currently leased (lease_expires_at > NOW())
	JobStatusCancelled      JobStatus = "CANCELLED"       // Cancellation requested
	JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE" // Waiting for available_at timestamp
	JobStatusPendingJobs    JobStatus = "PENDING_JOBS"    // Blocked by wait_for dependencies
	JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"   // Too many consecutive lease expirations
	JobStatusExpired        JobStatus = "EXPIRED"         // Hit expires_at timestamp
	JobStatusReady          JobStatus = "READY"           // Ready to be leased
)

// JobStatusInfo contains the current status and metadata of a job
// Maps directly to fields from pgwf.jobs_with_status view and pgwf.jobs_archive table
type JobStatusInfo struct {
	// Core identification
	TenantID string
	JobID    string

	// Status (computed by the view for active jobs)
	Status JobStatus

	// Job configuration
	NextNeed     string
	WaitFor      []string
	SingletonKey *string
	Payload      json.RawMessage

	// Timing
	AvailableAt time.Time
	ExpiresAt   *time.Time // nil if 'infinity'
	CreatedAt   time.Time
	ArchivedAt  *time.Time // Only populated if job is archived

	// Lease information
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity' (no active lease)
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64

	// Cancellation
	CancelRequested   bool
	CancelRequestedBy *string
	CancelRequestedAt *time.Time
}

// JobExistence contains information about whether a job exists
type JobExistence struct {
	Exists   bool
	TenantID string
	JobID    string
}

// JobListItem contains summary information about a job (excluding payload for efficiency)
type JobListItem struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	SingletonKey           *string
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	ArchivedAt             *time.Time
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// ListJobsOptions specifies filtering and pagination options for listing jobs
type ListJobsOptions struct {
	// Filtering
	TenantID        string      // Filter by tenant (required for multi-tenant systems)
	Statuses        []JobStatus // Filter by status (empty = all statuses)
	JobTypePattern  string      // Filter by job type pattern (SQL LIKE pattern)
	SingletonKey    string      // Filter by singleton key
	CreatedAfter    *time.Time  // Filter by creation time
	CreatedBefore   *time.Time
	IncludeArchived bool // Whether to include archived jobs (default: false, only active)

	// Pagination
	Limit  int    // Max results to return (default: 100, max: 1000)
	Cursor string // Opaque cursor for pagination (empty for first page)

	// Sorting
	SortBy    SortField     // Field to sort by (default: CreatedAt)
	SortOrder SortDirection // ASC or DESC (default: DESC)
}

// SortField specifies which field to sort by
type SortField string

const (
	SortByCreatedAt   SortField = "created_at"
	SortByAvailableAt SortField = "available_at"
	SortByJobID       SortField = "job_id"
)

// SortDirection specifies sort direction
type SortDirection string

const (
	SortAsc  SortDirection = "ASC"
	SortDesc SortDirection = "DESC"
)

// ListJobsResult contains the results of a job listing query
type ListJobsResult struct {
	Jobs       []JobListItem
	NextCursor string // Empty if no more results
	HasMore    bool
}

// FindJobsOptions specifies criteria for finding jobs
type FindJobsOptions struct {
	TenantIDs []string  // Filter by tenant IDs (empty = all tenants)
	Status    JobStatus // Required: status to match
	NextNeed  string    // Required: the capability the job is waiting for
	Limit     int       // Max results (default: 100, max: 1000)
}

// JobInfo contains detailed information about a job (without payload)
type JobInfo struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	SingletonKey           *string
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// GetJobOptions specifies options for retrieving a job
type GetJobOptions struct {
	IncludePayload bool // Whether to include the job payload (default: false)
}

// JobDetail contains complete information about a job including payload
type JobDetail struct {
	TenantID               string
	JobID                  string
	Status                 JobStatus
	NextNeed               string
	WaitFor                []string
	SingletonKey           *string
	Payload                json.RawMessage // Only populated if IncludePayload option is true
	AvailableAt            time.Time
	ExpiresAt              *time.Time // nil if 'infinity'
	CreatedAt              time.Time
	ArchivedAt             *time.Time // Only populated if querying archive
	LeaseID                *string
	LeaseExpiresAt         *time.Time // nil if '-infinity'
	LeaseExpirationCount   int64
	ConsecutiveExpirations int64
	CancelRequested        bool
	CancelRequestedBy      *string
	CancelRequestedAt      *time.Time
}

// ListArchivedJobsOptions specifies options for listing archived jobs
type ListArchivedJobsOptions struct {
	TenantID       string
	JobTypePattern string
	ArchivedAfter  *time.Time
	ArchivedBefore *time.Time
	Limit          int
	Cursor         string
}

var (
	// ErrTenantMismatch is returned when a job exists but belongs to a different tenant
	ErrTenantMismatch = fmt.Errorf("pgwf: job belongs to different tenant")

	// ErrInvalidCursor is returned when a pagination cursor is invalid or expired
	ErrInvalidCursor = fmt.Errorf("pgwf: invalid or expired cursor")

	// ErrInvalidOptions is returned when query options are invalid
	ErrInvalidOptions = fmt.Errorf("pgwf: invalid query options")
)

// GetJobStatus retrieves the status and metadata of a job by ID.
// Returns ErrJobNotFound if the job doesn't exist in either active or archived tables.
// Searches both pgwf.jobs_with_status (active) and pgwf.jobs_archive (archived).
func GetJobStatus(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobStatusInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgwf: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgwf: job id is required")
	}

	// Try active jobs first
	const activeQuery = `
		SELECT
			tenant_id, job_id, status, next_need, wait_for, singleton_key, payload,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_with_status
		WHERE tenant_id = $1 AND job_id = $2
	`

	info, err := scanJobStatusInfo(db.QueryRowContext(ctx, activeQuery, string(tenantID), string(jobID)))
	if err == nil {
		return info, nil
	}
	if err != sql.ErrNoRows {
		return nil, annotateError(err)
	}

	// Try archived jobs
	const archiveQuery = `
		SELECT
			tenant_id, job_id, next_need, wait_for, singleton_key, payload,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_archive
		WHERE tenant_id = $1 AND job_id = $2
	`

	info, err = scanJobStatusInfoArchive(db.QueryRowContext(ctx, archiveQuery, string(tenantID), string(jobID)))
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found in tenant %s", jobID, tenantID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	return info, nil
}

// scanJobStatusInfo scans a row from jobs_with_status into JobStatusInfo
func scanJobStatusInfo(row *sql.Row) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var archivedAt sql.NullTime
	var expiresAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&info.TenantID,
		&info.JobID,
		&status,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&info.Payload,
		&info.AvailableAt,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&leaseID,
		&leaseExpiresAt,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	info.Status = JobStatus(status)
	info.WaitFor = waitFor
	if singletonKey.Valid {
		info.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		info.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &info, nil
}

// scanJobStatusInfoArchive scans a row from jobs_archive into JobStatusInfo
func scanJobStatusInfoArchive(row *sql.Row) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&info.TenantID,
		&info.JobID,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&info.Payload,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&leaseID,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	// Archived jobs: status is CANCELLED if cancel_requested, otherwise assume completed
	if info.CancelRequested {
		info.Status = JobStatusCancelled
	} else {
		// We don't have a COMPLETED status in the view, so we infer it for archived jobs
		// that weren't cancelled
		info.Status = JobStatus("COMPLETED")
	}

	info.WaitFor = waitFor
	if singletonKey.Valid {
		info.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	info.AvailableAt = info.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &info, nil
}

// CheckJobExists checks if a job exists in either active or archived state.
func CheckJobExists(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobExistence, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgwf: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgwf: job id is required")
	}

	const query = `
		SELECT EXISTS(SELECT 1 FROM pgwf.jobs WHERE tenant_id = $1 AND job_id = $2)
		   OR EXISTS(SELECT 1 FROM pgwf.jobs_archive WHERE tenant_id = $1 AND job_id = $2)
	`

	var exists bool
	err := db.QueryRowContext(ctx, query, string(tenantID), string(jobID)).Scan(&exists)
	if err != nil {
		return nil, annotateError(err)
	}

	return &JobExistence{
		Exists:   exists,
		TenantID: string(tenantID),
		JobID:    string(jobID),
	}, nil
}

// CheckJobExistsWithTenant checks if a job exists and validates it belongs to the expected tenant.
// Returns ErrJobNotFound if job doesn't exist.
// Returns ErrTenantMismatch if job exists but belongs to different tenant.
func CheckJobExistsWithTenant(ctx context.Context, db DB, jobID JobID, expectedTenantID TenantID) (*JobExistence, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if expectedTenantID == "" {
		return nil, fmt.Errorf("pgwf: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgwf: job id is required")
	}

	const query = `
		SELECT tenant_id FROM pgwf.jobs WHERE job_id = $1
		UNION ALL
		SELECT tenant_id FROM pgwf.jobs_archive WHERE job_id = $1
		LIMIT 1
	`

	var actualTenantID string
	err := db.QueryRowContext(ctx, query, string(jobID)).Scan(&actualTenantID)
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found", jobID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	if actualTenantID != string(expectedTenantID) {
		return nil, wrap(ErrTenantMismatch, fmt.Errorf("job %s belongs to tenant %s, expected %s", jobID, actualTenantID, expectedTenantID))
	}

	return &JobExistence{
		Exists:   true,
		TenantID: actualTenantID,
		JobID:    string(jobID),
	}, nil
}

// GetJob retrieves a job by ID without leasing it.
// Returns ErrJobNotFound if the job doesn't exist.
// Searches both active and archived jobs.
func GetJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, opts GetJobOptions) (*JobDetail, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgwf: tenant id is required")
	}
	if jobID == "" {
		return nil, fmt.Errorf("pgwf: job id is required")
	}

	payloadCol := "NULL as payload"
	if opts.IncludePayload {
		payloadCol = "payload"
	}

	// Try active jobs first
	activeQuery := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, status, next_need, wait_for, singleton_key, %s,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_with_status
		WHERE tenant_id = $1 AND job_id = $2
	`, payloadCol)

	detail, err := scanJobDetail(db.QueryRowContext(ctx, activeQuery, string(tenantID), string(jobID)))
	if err == nil {
		return detail, nil
	}
	if err != sql.ErrNoRows {
		return nil, annotateError(err)
	}

	// Try archived jobs
	archiveQuery := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, next_need, wait_for, singleton_key, %s,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_archive
		WHERE tenant_id = $1 AND job_id = $2
	`, payloadCol)

	detail, err = scanJobDetailArchive(db.QueryRowContext(ctx, archiveQuery, string(tenantID), string(jobID)))
	if err == sql.ErrNoRows {
		return nil, wrap(ErrJobNotFound, fmt.Errorf("job %s not found in tenant %s", jobID, tenantID))
	}
	if err != nil {
		return nil, annotateError(err)
	}

	return detail, nil
}

// scanJobDetail scans a row from jobs_with_status into JobDetail
func scanJobDetail(row *sql.Row) (*JobDetail, error) {
	var detail JobDetail
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var payload sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&detail.TenantID,
		&detail.JobID,
		&status,
		&detail.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&payload,
		&detail.AvailableAt,
		&expiresAt,
		&detail.CreatedAt,
		&archivedAt,
		&leaseID,
		&leaseExpiresAt,
		&detail.LeaseExpirationCount,
		&detail.ConsecutiveExpirations,
		&detail.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	detail.Status = JobStatus(status)
	detail.WaitFor = waitFor
	if singletonKey.Valid {
		detail.SingletonKey = &singletonKey.String
	}
	if payload.Valid {
		detail.Payload = json.RawMessage(payload.String)
	}
	if expiresAt.Valid {
		detail.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		detail.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		detail.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		detail.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		detail.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		detail.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &detail, nil
}

// scanJobDetailArchive scans a row from jobs_archive into JobDetail
func scanJobDetailArchive(row *sql.Row) (*JobDetail, error) {
	var detail JobDetail
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var payload sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := row.Scan(
		&detail.TenantID,
		&detail.JobID,
		&detail.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&payload,
		&expiresAt,
		&detail.CreatedAt,
		&archivedAt,
		&leaseID,
		&detail.LeaseExpirationCount,
		&detail.ConsecutiveExpirations,
		&detail.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	// Archived jobs: status is CANCELLED if cancel_requested, otherwise completed
	if detail.CancelRequested {
		detail.Status = JobStatusCancelled
	} else {
		detail.Status = JobStatus("COMPLETED")
	}

	detail.WaitFor = waitFor
	if singletonKey.Valid {
		detail.SingletonKey = &singletonKey.String
	}
	if payload.Valid {
		detail.Payload = json.RawMessage(payload.String)
	}
	if expiresAt.Valid {
		detail.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		detail.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		detail.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		detail.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		detail.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	detail.AvailableAt = detail.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &detail, nil
}

// IsJobArchived checks if a specific job has been archived (completed).
// Returns true if job is in archive, false if in active jobs or doesn't exist.
func IsJobArchived(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return false, fmt.Errorf("pgwf: nil context")
	}
	if tenantID == "" {
		return false, fmt.Errorf("pgwf: tenant id is required")
	}
	if jobID == "" {
		return false, fmt.Errorf("pgwf: job id is required")
	}

	const query = `
		SELECT EXISTS(SELECT 1 FROM pgwf.jobs_archive WHERE tenant_id = $1 AND job_id = $2)
	`

	var archived bool
	err := db.QueryRowContext(ctx, query, string(tenantID), string(jobID)).Scan(&archived)
	if err != nil {
		return false, annotateError(err)
	}

	return archived, nil
}

// FindJobs finds jobs matching specific criteria.
// This is useful for discovering work without leasing it (e.g., external task workers).
// Returns jobs sorted by AvailableAt ascending.
func FindJobs(ctx context.Context, db DB, opts FindJobsOptions) ([]JobInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if opts.NextNeed == "" {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("next_need is required"))
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Build WHERE clause for tenant filtering
	var tenantFilter string
	var args []interface{}
	argIdx := 1

	if len(opts.TenantIDs) > 0 {
		tenantFilter = fmt.Sprintf("tenant_id = ANY($%d) AND ", argIdx)
		args = append(args, pq.Array(opts.TenantIDs))
		argIdx++
	}

	// Add status and next_need filters
	query := fmt.Sprintf(`
		SELECT
			tenant_id, job_id, status, next_need, wait_for, singleton_key,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_with_status
		WHERE %sstatus = $%d AND next_need = $%d
		ORDER BY available_at ASC
		LIMIT $%d
	`, tenantFilter, argIdx, argIdx+1, argIdx+2)

	args = append(args, string(opts.Status), opts.NextNeed, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobInfo
	for rows.Next() {
		job, err := scanJobInfo(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	return jobs, nil
}

// scanJobInfo scans a row into JobInfo
func scanJobInfo(rows interface{ Scan(...interface{}) error }) (*JobInfo, error) {
	var job JobInfo
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&leaseID,
		&leaseExpiresAt,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	if singletonKey.Valid {
		job.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &job, nil
}

// GetJobStatusBatch retrieves status for multiple jobs in a single query.
// Returns a map of jobID -> JobStatusInfo.
// Jobs that don't exist are omitted from the result.
func GetJobStatusBatch(ctx context.Context, db DB, tenantID TenantID, jobIDs []JobID) (map[string]*JobStatusInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("pgwf: tenant id is required")
	}
	if len(jobIDs) == 0 {
		return make(map[string]*JobStatusInfo), nil
	}

	// Convert JobID slice to string slice
	jobIDStrings := make([]string, len(jobIDs))
	for i, id := range jobIDs {
		jobIDStrings[i] = string(id)
	}

	// Query active jobs
	const activeQuery = `
		SELECT
			tenant_id, job_id, status, next_need, wait_for, singleton_key, payload,
			available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at,
			NULL as archived_at,
			lease_id,
			CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
			lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_with_status
		WHERE tenant_id = $1 AND job_id = ANY($2)
	`

	result := make(map[string]*JobStatusInfo)

	rows, err := db.QueryContext(ctx, activeQuery, string(tenantID), pq.Array(jobIDStrings))
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	for rows.Next() {
		info, err := scanJobStatusInfoRows(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		result[info.JobID] = info
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Query archived jobs for any not found in active
	const archiveQuery = `
		SELECT
			tenant_id, job_id, next_need, wait_for, singleton_key, payload,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_archive
		WHERE tenant_id = $1 AND job_id = ANY($2)
	`

	rows, err = db.QueryContext(ctx, archiveQuery, string(tenantID), pq.Array(jobIDStrings))
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	for rows.Next() {
		info, err := scanJobStatusInfoArchiveRows(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		// Only add if not already found in active jobs
		if _, exists := result[info.JobID]; !exists {
			result[info.JobID] = info
		}
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	return result, nil
}

// scanJobStatusInfoRows scans a row from jobs_with_status into JobStatusInfo
func scanJobStatusInfoRows(rows interface{ Scan(...interface{}) error }) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&info.TenantID,
		&info.JobID,
		&status,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&info.Payload,
		&info.AvailableAt,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&leaseID,
		&leaseExpiresAt,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	info.Status = JobStatus(status)
	info.WaitFor = waitFor
	if singletonKey.Valid {
		info.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		info.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &info, nil
}

// scanJobStatusInfoArchiveRows scans a row from jobs_archive into JobStatusInfo
func scanJobStatusInfoArchiveRows(rows interface{ Scan(...interface{}) error }) (*JobStatusInfo, error) {
	var info JobStatusInfo
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&info.TenantID,
		&info.JobID,
		&info.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&info.Payload,
		&expiresAt,
		&info.CreatedAt,
		&archivedAt,
		&leaseID,
		&info.LeaseExpirationCount,
		&info.ConsecutiveExpirations,
		&info.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	// Archived jobs: status is CANCELLED if cancel_requested, otherwise completed
	if info.CancelRequested {
		info.Status = JobStatusCancelled
	} else {
		info.Status = JobStatus("COMPLETED")
	}

	info.WaitFor = waitFor
	if singletonKey.Valid {
		info.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		info.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		info.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		info.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		info.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs: AvailableAt not stored, use CreatedAt as fallback
	info.AvailableAt = info.CreatedAt
	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &info, nil
}

// ListJobs queries jobs with filtering and pagination.
// The cursor returned in ListJobsResult can be used for the next page.
func ListJobs(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobsResult, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if opts.TenantID == "" {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("tenant_id is required"))
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	sortBy := opts.SortBy
	if sortBy == "" {
		sortBy = SortByCreatedAt
	}

	sortOrder := opts.SortOrder
	if sortOrder == "" {
		sortOrder = SortDesc
	}

	// Build WHERE clause
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Always filter by tenant
	conditions = append(conditions, fmt.Sprintf("tenant_id = $%d", argIdx))
	args = append(args, opts.TenantID)
	argIdx++

	// Status filter
	if len(opts.Statuses) > 0 {
		statusStrings := make([]string, len(opts.Statuses))
		for i, s := range opts.Statuses {
			statusStrings[i] = string(s)
		}
		conditions = append(conditions, fmt.Sprintf("status = ANY($%d)", argIdx))
		args = append(args, pq.Array(statusStrings))
		argIdx++
	}

	// Job type pattern filter
	if opts.JobTypePattern != "" {
		conditions = append(conditions, fmt.Sprintf("next_need LIKE $%d", argIdx))
		args = append(args, opts.JobTypePattern)
		argIdx++
	}

	// Singleton key filter
	if opts.SingletonKey != "" {
		conditions = append(conditions, fmt.Sprintf("singleton_key = $%d", argIdx))
		args = append(args, opts.SingletonKey)
		argIdx++
	}

	// Created after filter
	if opts.CreatedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("created_at > $%d", argIdx))
		args = append(args, *opts.CreatedAfter)
		argIdx++
	}

	// Created before filter
	if opts.CreatedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("created_at < $%d", argIdx))
		args = append(args, *opts.CreatedBefore)
		argIdx++
	}

	whereClause := "WHERE " + conditions[0]
	for i := 1; i < len(conditions); i++ {
		whereClause += " AND " + conditions[i]
	}

	// Build query - query active jobs only by default unless IncludeArchived is true
	var query string
	if opts.IncludeArchived {
		// Query both active and archived with UNION
		query = fmt.Sprintf(`
			SELECT
				tenant_id, job_id, status, next_need, wait_for, singleton_key,
				available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				NULL as archived_at,
				lease_id,
				CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgwf.jobs_with_status
			%s
			UNION ALL
			SELECT
				tenant_id, job_id,
				CASE WHEN cancel_requested THEN 'CANCELLED' ELSE 'COMPLETED' END as status,
				next_need, wait_for, singleton_key,
				created_at as available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				archived_at,
				lease_id,
				NULL as lease_expires_at,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgwf.jobs_archive
			%s
			ORDER BY %s %s
			LIMIT $%d
		`, whereClause, whereClause, sortBy, sortOrder, argIdx)
	} else {
		// Query only active jobs
		query = fmt.Sprintf(`
			SELECT
				tenant_id, job_id, status, next_need, wait_for, singleton_key,
				available_at,
				CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
				created_at,
				NULL as archived_at,
				lease_id,
				CASE WHEN lease_expires_at = '-infinity' THEN NULL ELSE lease_expires_at END,
				lease_expiration_count, consecutive_expirations,
				cancel_requested, cancel_requested_by, cancel_requested_at
			FROM pgwf.jobs_with_status
			%s
			ORDER BY %s %s
			LIMIT $%d
		`, whereClause, sortBy, sortOrder, argIdx)
	}

	args = append(args, limit+1) // Fetch one extra to determine if there are more results

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobListItem
	for rows.Next() {
		job, err := scanJobListItem(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Determine if there are more results
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit] // Remove the extra row
	}

	result := &ListJobsResult{
		Jobs:    jobs,
		HasMore: hasMore,
	}

	// For now, leave cursor implementation as empty string
	// Full cursor-based pagination can be added later if needed
	if hasMore {
		result.NextCursor = "next-page" // Placeholder
	}

	return result, nil
}

// scanJobListItem scans a row into JobListItem
func scanJobListItem(rows interface{ Scan(...interface{}) error }) (*JobListItem, error) {
	var job JobListItem
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var archivedAt sql.NullTime
	var leaseID sql.NullString
	var leaseExpiresAt sql.NullTime
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&archivedAt,
		&leaseID,
		&leaseExpiresAt,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	if singletonKey.Valid {
		job.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if archivedAt.Valid {
		job.ArchivedAt = &archivedAt.Time
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	return &job, nil
}

// ListArchivedJobs queries only archived jobs with pagination.
// This is more efficient than ListJobs when you only need completed jobs.
func ListArchivedJobs(ctx context.Context, db DB, opts ListArchivedJobsOptions) (*ListJobsResult, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if opts.TenantID == "" {
		return nil, wrap(ErrInvalidOptions, fmt.Errorf("tenant_id is required"))
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Build WHERE clause
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Always filter by tenant
	conditions = append(conditions, fmt.Sprintf("tenant_id = $%d", argIdx))
	args = append(args, opts.TenantID)
	argIdx++

	// Job type pattern filter
	if opts.JobTypePattern != "" {
		conditions = append(conditions, fmt.Sprintf("next_need LIKE $%d", argIdx))
		args = append(args, opts.JobTypePattern)
		argIdx++
	}

	// Archived after filter
	if opts.ArchivedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("archived_at > $%d", argIdx))
		args = append(args, *opts.ArchivedAfter)
		argIdx++
	}

	// Archived before filter
	if opts.ArchivedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("archived_at < $%d", argIdx))
		args = append(args, *opts.ArchivedBefore)
		argIdx++
	}

	whereClause := "WHERE " + conditions[0]
	for i := 1; i < len(conditions); i++ {
		whereClause += " AND " + conditions[i]
	}

	query := fmt.Sprintf(`
		SELECT
			tenant_id, job_id,
			CASE WHEN cancel_requested THEN 'CANCELLED' ELSE 'COMPLETED' END as status,
			next_need, wait_for, singleton_key,
			created_at as available_at,
			CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END,
			created_at, archived_at,
			lease_id, lease_expiration_count, consecutive_expirations,
			cancel_requested, cancel_requested_by, cancel_requested_at
		FROM pgwf.jobs_archive
		%s
		ORDER BY archived_at DESC
		LIMIT $%d
	`, whereClause, argIdx)

	args = append(args, limit+1) // Fetch one extra to determine if there are more results

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, annotateError(err)
	}
	defer rows.Close()

	var jobs []JobListItem
	for rows.Next() {
		job, err := scanJobListItemArchive(rows)
		if err != nil {
			return nil, annotateError(err)
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, annotateError(err)
	}

	// Determine if there are more results
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}

	result := &ListJobsResult{
		Jobs:    jobs,
		HasMore: hasMore,
	}

	if hasMore {
		result.NextCursor = "next-page" // Placeholder
	}

	return result, nil
}

// scanJobListItemArchive scans a row from jobs_archive into JobListItem
func scanJobListItemArchive(rows interface{ Scan(...interface{}) error }) (*JobListItem, error) {
	var job JobListItem
	var status string
	var waitFor pq.StringArray
	var singletonKey sql.NullString
	var expiresAt sql.NullTime
	var leaseID sql.NullString
	var cancelRequestedBy sql.NullString
	var cancelRequestedAt sql.NullTime

	err := rows.Scan(
		&job.TenantID,
		&job.JobID,
		&status,
		&job.NextNeed,
		(*pq.StringArray)(&waitFor),
		&singletonKey,
		&job.AvailableAt,
		&expiresAt,
		&job.CreatedAt,
		&job.ArchivedAt,
		&leaseID,
		&job.LeaseExpirationCount,
		&job.ConsecutiveExpirations,
		&job.CancelRequested,
		&cancelRequestedBy,
		&cancelRequestedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatus(status)
	job.WaitFor = waitFor
	if singletonKey.Valid {
		job.SingletonKey = &singletonKey.String
	}
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}
	if leaseID.Valid {
		job.LeaseID = &leaseID.String
	}
	if cancelRequestedBy.Valid {
		job.CancelRequestedBy = &cancelRequestedBy.String
	}
	if cancelRequestedAt.Valid {
		job.CancelRequestedAt = &cancelRequestedAt.Time
	}

	// Archived jobs have no active lease, leave LeaseExpiresAt as nil

	return &job, nil
}
