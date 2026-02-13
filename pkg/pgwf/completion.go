package pgwf

import "fmt"

// CompletionStatus represents the outcome stored in pgwf.jobs_archive.
type CompletionStatus string

const (
	CompletionStatusSucceeded CompletionStatus = "succeeded"
	CompletionStatusFailed    CompletionStatus = "failed"
	CompletionStatusCancelled CompletionStatus = "cancelled"
)

func normalizeCompletion(status CompletionStatus, failureDetail string) (CompletionStatus, any, error) {
	if status == "" {
		status = CompletionStatusSucceeded
	}
	switch status {
	case CompletionStatusSucceeded, CompletionStatusFailed:
	default:
		return "", nil, fmt.Errorf("pgwf: completion status must be succeeded or failed")
	}
	if failureDetail != "" && status != CompletionStatusFailed {
		return "", nil, fmt.Errorf("pgwf: failure detail is only allowed when completion status is failed")
	}
	if failureDetail == "" {
		return status, nil, nil
	}
	return status, failureDetail, nil
}
