package pgwf

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

const (
	completeUnheldStmt   = `SELECT pgwf.complete_unheld_job($1, $2)`
	rescheduleUnheldStmt = `
SELECT job_id, next_need, wait_for, available_at
FROM pgwf.reschedule_unheld_job($1, $2, $3, $4, $5, $6)
`
)

// CompleteUnheldJob finalizes a job without requiring a lease by locking it directly.
func CompleteUnheldJob(ctx context.Context, db DB, jobID JobID, worker WorkerID) error {
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

	row := db.QueryRowContext(ctx, completeUnheldStmt, string(jobID), string(worker))
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return annotateError(err)
	}
	if !ok {
		return wrap(ErrJobNotFound, fmt.Errorf("pgwf: complete_unheld_job returned false"))
	}
	return nil
}

// RescheduleUnheldJob updates the dependencies/availability for a ready job without re-leasing.
func RescheduleUnheldJob(ctx context.Context, db DB, jobID JobID, worker WorkerID, deps JobDependencies) error {
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
	if err := deps.validate(); err != nil {
		return err
	}

	row := db.QueryRowContext(ctx, rescheduleUnheldStmt,
		string(jobID),
		string(worker),
		string(deps.NextNeed),
		pq.Array(deps.waitForStrings()),
		deps.SingletonKey,
		deps.availableAtArg(),
	)

	var (
		id        string
		need      string
		waits     pq.StringArray
		available sql.NullTime
	)
	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &available); err != nil {
		return annotateError(err)
	}
	return nil
}
