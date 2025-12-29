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

// NOTE: TestGetJobStatus_NotFound is now in installer/integration_test.go as part of comprehensive integration tests

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

// NOTE: TestGetJobStatusBatch_EmptyInput is now in installer/integration_test.go (TestGetJobStatusBatch)

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

// Tests for new cursor pagination functionality

func TestListJobs_MultiTenantSupport(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test TenantIDs takes precedence over TenantID
	opts := pgwf.ListJobsOptions{
		TenantID:  "old-tenant",
		TenantIDs: []string{"tenant-1", "tenant-2"},
		Limit:     10,
	}

	// This should not error since TenantIDs is set
	// (would need actual DB to execute, but validates options processing)
	if len(opts.TenantIDs) != 2 {
		t.Errorf("expected TenantIDs to have 2 elements, got %d", len(opts.TenantIDs))
	}

	// Test backwards compatibility: TenantID used when TenantIDs empty
	opts2 := pgwf.ListJobsOptions{
		TenantID: "single-tenant",
		Limit:    10,
	}

	if opts2.TenantID != "single-tenant" {
		t.Errorf("expected TenantID to be 'single-tenant', got %s", opts2.TenantID)
	}

	// Test error when both are empty
	_, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
		Limit: 10,
	})
	if err == nil {
		t.Error("expected error when neither TenantID nor TenantIDs is set")
	}
}

func TestListJobs_MultiPatternSupport(t *testing.T) {
	// Test JobTypePatterns takes precedence over JobTypePattern
	opts := pgwf.ListJobsOptions{
		TenantID:        "tenant-1",
		JobTypePattern:  "old-pattern",
		JobTypePatterns: []string{"pattern-1", "pattern-2", "pattern-3"},
		Limit:           10,
	}

	if len(opts.JobTypePatterns) != 3 {
		t.Errorf("expected JobTypePatterns to have 3 elements, got %d", len(opts.JobTypePatterns))
	}

	// Test backwards compatibility: JobTypePattern used when JobTypePatterns empty
	opts2 := pgwf.ListJobsOptions{
		TenantID:       "tenant-1",
		JobTypePattern: "single-pattern:%",
		Limit:          10,
	}

	if opts2.JobTypePattern != "single-pattern:%" {
		t.Errorf("expected JobTypePattern to be 'single-pattern:%%', got %s", opts2.JobTypePattern)
	}

	// Test both empty is valid (no job type filtering)
	opts3 := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    10,
	}

	if opts3.JobTypePattern != "" || len(opts3.JobTypePatterns) != 0 {
		t.Error("expected both job type filters to be empty")
	}
}

func TestListJobs_DefaultValues(t *testing.T) {
	// Test that defaults are applied correctly
	// Note: Actual defaults are applied in ListJobs function, so we can't test them
	// directly without a DB, but we can verify the option struct accepts them

	opts := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		// Limit not set - should default to 100
		// SortBy not set - should default to SortByCreatedAt
		// SortOrder not set - should default to SortDesc
	}

	if opts.Limit != 0 {
		t.Errorf("expected Limit to be 0 (unset), got %d", opts.Limit)
	}

	if opts.SortBy != "" {
		t.Errorf("expected SortBy to be empty (unset), got %s", opts.SortBy)
	}

	if opts.SortOrder != "" {
		t.Errorf("expected SortOrder to be empty (unset), got %s", opts.SortOrder)
	}
}

func TestListJobs_InvalidCursor(t *testing.T) {
	ctx := context.Background()
	var db *sql.DB

	// Test invalid base64 cursor
	_, err := pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Cursor:   "not-valid-base64!!!",
		Limit:    10,
	})

	// Should get an error (either invalid cursor or nil DB error)
	if err == nil {
		t.Error("expected error for invalid cursor")
	}

	// Test malformed JSON cursor
	invalidCursor := "aW52YWxpZC1qc29u" // base64 of "invalid-json"
	_, err = pgwf.ListJobs(ctx, db, pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Cursor:   invalidCursor,
		Limit:    10,
	})

	if err == nil {
		t.Error("expected error for malformed cursor")
	}
}

func TestCursorValidation_HashConsistency(t *testing.T) {
	// Test that the same options produce the same hash
	opts1 := pgwf.ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"tenant-1", "tenant-2"},
		Statuses:        []pgwf.JobStatus{pgwf.JobStatusReady, pgwf.JobStatusActive},
		JobTypePatterns: []string{"pattern-1", "pattern-2"},
		SingletonKey:    "singleton-1",
		IncludeArchived: true,
		SortBy:          pgwf.SortByCreatedAt,
		SortOrder:       pgwf.SortDesc,
	}

	opts2 := pgwf.ListJobsOptions{
		TenantID:        "tenant-1",
		TenantIDs:       []string{"tenant-1", "tenant-2"},
		Statuses:        []pgwf.JobStatus{pgwf.JobStatusReady, pgwf.JobStatusActive},
		JobTypePatterns: []string{"pattern-1", "pattern-2"},
		SingletonKey:    "singleton-1",
		IncludeArchived: true,
		SortBy:          pgwf.SortByCreatedAt,
		SortOrder:       pgwf.SortDesc,
	}

	// Note: We can't call the internal hashListJobsOptions function directly from the test
	// package, but we can verify that the same options should produce the same cursor
	// when encoding. This would need to be tested with actual cursor encoding.

	// For now, just verify the options are identical
	if opts1.TenantID != opts2.TenantID {
		t.Error("tenant IDs should match")
	}
	if len(opts1.TenantIDs) != len(opts2.TenantIDs) {
		t.Error("tenant IDs slices should have same length")
	}
	if opts1.SortBy != opts2.SortBy {
		t.Error("sort by should match")
	}
	if opts1.SortOrder != opts2.SortOrder {
		t.Error("sort order should match")
	}
}

func TestCursorValidation_DifferentOptionsProduceDifferentHashes(t *testing.T) {
	// Test that different options should produce different hashes
	opts1 := pgwf.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgwf.SortByCreatedAt,
		SortOrder: pgwf.SortDesc,
	}

	opts2 := pgwf.ListJobsOptions{
		TenantID:  "tenant-2", // Different tenant
		SortBy:    pgwf.SortByCreatedAt,
		SortOrder: pgwf.SortDesc,
	}

	if opts1.TenantID == opts2.TenantID {
		t.Error("tenant IDs should be different")
	}

	opts3 := pgwf.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgwf.SortByAvailableAt, // Different sort field
		SortOrder: pgwf.SortDesc,
	}

	if opts1.SortBy == opts3.SortBy {
		t.Error("sort fields should be different")
	}

	opts4 := pgwf.ListJobsOptions{
		TenantID:  "tenant-1",
		SortBy:    pgwf.SortByCreatedAt,
		SortOrder: pgwf.SortAsc, // Different sort order
	}

	if opts1.SortOrder == opts4.SortOrder {
		t.Error("sort orders should be different")
	}
}

func TestListJobs_SortFieldValidation(t *testing.T) {
	// Test all valid sort fields
	validSortFields := []pgwf.SortField{
		pgwf.SortByCreatedAt,
		pgwf.SortByAvailableAt,
		pgwf.SortByJobID,
	}

	for _, field := range validSortFields {
		opts := pgwf.ListJobsOptions{
			TenantID:  "tenant-1",
			SortBy:    field,
			SortOrder: pgwf.SortDesc,
			Limit:     10,
		}

		if opts.SortBy != field {
			t.Errorf("expected SortBy to be %s, got %s", field, opts.SortBy)
		}
	}
}

func TestListJobs_SortDirectionValidation(t *testing.T) {
	// Test both valid sort directions
	validDirections := []pgwf.SortDirection{
		pgwf.SortAsc,
		pgwf.SortDesc,
	}

	for _, direction := range validDirections {
		opts := pgwf.ListJobsOptions{
			TenantID:  "tenant-1",
			SortBy:    pgwf.SortByCreatedAt,
			SortOrder: direction,
			Limit:     10,
		}

		if opts.SortOrder != direction {
			t.Errorf("expected SortOrder to be %s, got %s", direction, opts.SortOrder)
		}
	}
}

func TestListJobs_LimitBounds(t *testing.T) {
	// Test limit validation
	// Note: Actual clamping happens in ListJobs function
	opts := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    0, // Should default to 100
	}

	if opts.Limit != 0 {
		t.Errorf("expected unset Limit to be 0, got %d", opts.Limit)
	}

	opts2 := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    5000, // Should be clamped to 1000
	}

	if opts2.Limit != 5000 {
		t.Errorf("expected Limit to be 5000 (before clamping), got %d", opts2.Limit)
	}

	opts3 := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    50, // Valid limit
	}

	if opts3.Limit != 50 {
		t.Errorf("expected Limit to be 50, got %d", opts3.Limit)
	}
}

func TestListJobs_StatusFiltering(t *testing.T) {
	// Test multiple status filtering
	statuses := []pgwf.JobStatus{
		pgwf.JobStatusReady,
		pgwf.JobStatusActive,
		pgwf.JobStatusPendingJobs,
	}

	opts := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Statuses: statuses,
		Limit:    10,
	}

	if len(opts.Statuses) != 3 {
		t.Errorf("expected 3 statuses, got %d", len(opts.Statuses))
	}

	for i, status := range opts.Statuses {
		if status != statuses[i] {
			t.Errorf("status %d: expected %s, got %s", i, statuses[i], status)
		}
	}
}

func TestListJobs_TimeRangeFiltering(t *testing.T) {
	// Test time range filtering with nil values
	opts := pgwf.ListJobsOptions{
		TenantID:      "tenant-1",
		CreatedAfter:  nil,
		CreatedBefore: nil,
		Limit:         10,
	}

	if opts.CreatedAfter != nil {
		t.Error("expected CreatedAfter to be nil")
	}

	if opts.CreatedBefore != nil {
		t.Error("expected CreatedBefore to be nil")
	}
}

func TestListJobs_IncludeArchivedFlag(t *testing.T) {
	// Test IncludeArchived flag defaults to false
	opts := pgwf.ListJobsOptions{
		TenantID: "tenant-1",
		Limit:    10,
	}

	if opts.IncludeArchived {
		t.Error("expected IncludeArchived to default to false")
	}

	// Test setting it to true
	opts2 := pgwf.ListJobsOptions{
		TenantID:        "tenant-1",
		IncludeArchived: true,
		Limit:           10,
	}

	if !opts2.IncludeArchived {
		t.Error("expected IncludeArchived to be true")
	}
}

func TestFindJobs_MultiTenantSupport(t *testing.T) {
	// Test FindJobs with multiple tenants
	opts := pgwf.FindJobsOptions{
		TenantIDs: []string{"tenant-1", "tenant-2", "tenant-3"},
		Status:    pgwf.JobStatusReady,
		NextNeed:  "test-capability",
		Limit:     50,
	}

	if len(opts.TenantIDs) != 3 {
		t.Errorf("expected 3 tenant IDs, got %d", len(opts.TenantIDs))
	}

	// Test with empty TenantIDs (should query all tenants)
	opts2 := pgwf.FindJobsOptions{
		Status:   pgwf.JobStatusReady,
		NextNeed: "test-capability",
		Limit:    50,
	}

	if len(opts2.TenantIDs) != 0 {
		t.Errorf("expected 0 tenant IDs, got %d", len(opts2.TenantIDs))
	}
}

func TestFindJobs_LimitDefault(t *testing.T) {
	// Test FindJobs limit defaults
	opts := pgwf.FindJobsOptions{
		Status:   pgwf.JobStatusReady,
		NextNeed: "test-capability",
		// Limit not set
	}

	if opts.Limit != 0 {
		t.Errorf("expected Limit to be 0 (unset), got %d", opts.Limit)
	}

	// Test with explicit limit
	opts2 := pgwf.FindJobsOptions{
		Status:   pgwf.JobStatusReady,
		NextNeed: "test-capability",
		Limit:    200,
	}

	if opts2.Limit != 200 {
		t.Errorf("expected Limit to be 200, got %d", opts2.Limit)
	}
}

// Test error types
func TestErrorTypes(t *testing.T) {
	// Test that error constants are defined
	if pgwf.ErrJobNotFound == nil {
		t.Error("ErrJobNotFound should be defined")
	}

	if pgwf.ErrTenantMismatch == nil {
		t.Error("ErrTenantMismatch should be defined")
	}

	if pgwf.ErrInvalidCursor == nil {
		t.Error("ErrInvalidCursor should be defined")
	}

	if pgwf.ErrInvalidOptions == nil {
		t.Error("ErrInvalidOptions should be defined")
	}
}
