package pgwf

import (
	"context"
	"fmt"
)

const cancelStmt = `
SELECT cancel_requested
FROM pgwf.cancel_job($1, $2, $3)
`

// CancelJob requests cancellation for the given job_id via pgwf.cancel_job.
// If the job is already cancelled, the function succeeds without error.
func CancelJob(ctx context.Context, db DB, jobID JobID, worker WorkerID, reason string) error {
	if db == nil {
		return fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return fmt.Errorf("pgwf: nil context")
	}
	if jobID == "" {
		return fmt.Errorf("pgwf: job id is required")
	}
	if worker == "" {
		return fmt.Errorf("pgwf: worker id is required")
	}

	var reasonArg any
	if reason != "" {
		reasonArg = reason
	}

	row := db.QueryRowContext(ctx, cancelStmt, string(jobID), string(worker), reasonArg)
	var cancelled bool
	if err := row.Scan(&cancelled); err != nil {
		return annotateError(err)
	}
	if !cancelled {
		return fmt.Errorf("pgwf: cancel_job returned false")
	}
	return nil
}
