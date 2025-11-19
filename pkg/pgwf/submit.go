package pgwf

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

const submitStmt = `
SELECT job_id, next_need, wait_for, available_at
FROM pgwf.submit_job($1, $2, $3, $4, $5, $6)
`

// SubmitJob inserts workflow metadata using pgwf.submit_job.
func SubmitJob(ctx context.Context, db DB, jobID JobID, deps JobDependencies, worker WorkerID) error {
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

	row := db.QueryRowContext(ctx, submitStmt,
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
