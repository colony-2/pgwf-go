package pgwf_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

// Note: These are unit tests that work with the real database setup from integration_test.go
// For full integration tests, see installer/integration_test.go

func TestGetJobStatus_NotFound(t *testing.T) {
	// This test will require a database setup
	// For now, it's a placeholder showing the API usage
	t.Skip("Requires database setup")

	ctx := context.Background()
	var db *sql.DB // Would be set up in actual test

	_, err := pgwf.GetJobStatus(ctx, db, "test-tenant", "nonexistent-job")
	if err != pgwf.ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestCheckJobExists_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing tenant ID
	_, err := pgwf.CheckJobExists(ctx, db, "", "job-1")
	if err == nil || err.Error() != "pgwf: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgwf.CheckJobExists(ctx, db, "tenant-1", "")
	if err == nil || err.Error() != "pgwf: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}

	// Test nil context
	_, err = pgwf.CheckJobExists(nil, db, "tenant-1", "job-1")
	if err == nil || err.Error() != "pgwf: nil context" {
		t.Errorf("expected nil context error, got %v", err)
	}

	// Test nil DB
	_, err = pgwf.CheckJobExists(ctx, nil, "tenant-1", "job-1")
	if err == nil || err.Error() != "pgwf: nil DB" {
		t.Errorf("expected nil DB error, got %v", err)
	}
}

func TestListJobsOptions_DefaultsAndLimits(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing tenant ID
	_, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{})
	if err == nil {
		t.Errorf("expected error for missing tenant_id")
	}

	// Test nil context
	_, err = pgwf.ListJobs(nil, db, pgwf.ListJobsOptions{TenantID: "test"})
	if err == nil || err.Error() != "pgwf: nil context" {
		t.Errorf("expected nil context error, got %v", err)
	}

	// Test nil DB
	_, err = pgwf.ListJobs(ctx, nil, pgwf.ListJobsOptions{TenantID: "test"})
	if err == nil || err.Error() != "pgwf: nil DB" {
		t.Errorf("expected nil DB error, got %v", err)
	}
}

func TestFindJobsOptions_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB // nil is fine for validation tests

	// Test missing next_need
	_, err := pgwf.FindJobs(ctx, db, pgwf.FindJobsOptions{
		Status: pgwf.JobStatusReady,
	})
	if err == nil {
		t.Errorf("expected error for missing next_need")
	}
}

func TestGetJobStatusBatch_EmptyInput(t *testing.T) {
	t.Skip("Requires database setup")

	ctx := context.Background()
	var db *sql.DB // Would be set up in actual test

	result, err := pgwf.GetJobStatusBatch(ctx, db, "test-tenant", []pgwf.JobID{})
	if err != nil {
		t.Errorf("expected no error for empty input, got %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result map, got %d entries", len(result))
	}
}

func TestIsJobArchived_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgwf.IsJobArchived(ctx, db, "", "job-1")
	if err == nil || err.Error() != "pgwf: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgwf.IsJobArchived(ctx, db, "tenant-1", "")
	if err == nil || err.Error() != "pgwf: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}

func TestGetJob_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgwf.GetJob(ctx, db, "", "job-1", pgwf.GetJobOptions{})
	if err == nil || err.Error() != "pgwf: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgwf.GetJob(ctx, db, "tenant-1", "", pgwf.GetJobOptions{})
	if err == nil || err.Error() != "pgwf: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}

func TestJobStatusConstants(t *testing.T) {
	// Verify all status constants are defined correctly
	statuses := []pgwf.JobStatus{
		pgwf.JobStatusActive,
		pgwf.JobStatusCancelled,
		pgwf.JobStatusAwaitingFuture,
		pgwf.JobStatusPendingJobs,
		pgwf.JobStatusCrashConcern,
		pgwf.JobStatusExpired,
		pgwf.JobStatusReady,
	}

	expectedValues := []string{
		"ACTIVE",
		"CANCELLED",
		"AWAITING_FUTURE",
		"PENDING_JOBS",
		"CRASH_CONCERN",
		"EXPIRED",
		"READY",
	}

	for i, status := range statuses {
		if string(status) != expectedValues[i] {
			t.Errorf("status %d: expected %s, got %s", i, expectedValues[i], status)
		}
	}
}

func TestSortFieldConstants(t *testing.T) {
	// Verify sort field constants
	if pgwf.SortByCreatedAt != "created_at" {
		t.Errorf("SortByCreatedAt should be 'created_at', got %s", pgwf.SortByCreatedAt)
	}
	if pgwf.SortByAvailableAt != "available_at" {
		t.Errorf("SortByAvailableAt should be 'available_at', got %s", pgwf.SortByAvailableAt)
	}
	if pgwf.SortByJobID != "job_id" {
		t.Errorf("SortByJobID should be 'job_id', got %s", pgwf.SortByJobID)
	}
}

func TestSortDirectionConstants(t *testing.T) {
	// Verify sort direction constants
	if pgwf.SortAsc != "ASC" {
		t.Errorf("SortAsc should be 'ASC', got %s", pgwf.SortAsc)
	}
	if pgwf.SortDesc != "DESC" {
		t.Errorf("SortDesc should be 'DESC', got %s", pgwf.SortDesc)
	}
}

func TestListArchivedJobs_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgwf.ListArchivedJobs(ctx, db, pgwf.ListArchivedJobsOptions{})
	if err == nil {
		t.Errorf("expected error for missing tenant_id")
	}
}

func TestCheckJobExistsWithTenant_Validation(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test missing tenant ID
	_, err := pgwf.CheckJobExistsWithTenant(ctx, db, "job-1", "")
	if err == nil || err.Error() != "pgwf: tenant id is required" {
		t.Errorf("expected tenant id required error, got %v", err)
	}

	// Test missing job ID
	_, err = pgwf.CheckJobExistsWithTenant(ctx, db, "", "tenant-1")
	if err == nil || err.Error() != "pgwf: job id is required" {
		t.Errorf("expected job id required error, got %v", err)
	}
}
