package pgwf

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const getWorkStmt = `
SELECT job_id, lease_id, next_need, singleton_key, wait_for, available_at, lease_expires_at
FROM pgwf.get_work($1, $2, $3, $4)
`

// GetWork attempts to fetch a single job lease matching the provided capabilities.
func GetWork(ctx context.Context, db DB, worker WorkerID, capabilities []Capability) (*Lease, error) {
	if db == nil {
		return nil, fmt.Errorf("pgwf: nil DB")
	}
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	if worker == "" {
		return nil, fmt.Errorf("pgwf: worker id required")
	}
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("pgwf: at least one capability is required")
	}

	caps := capabilitiesToStrings(capabilities)
	row := db.QueryRowContext(ctx, getWorkStmt, string(worker), pq.Array(caps), defaultLeaseSeconds, 1)

	var (
		jobID     string
		leaseID   string
		need      string
		singleton sql.NullString
		waits     pq.StringArray
		available time.Time
		expires   time.Time
	)

	if err := row.Scan(&jobID, &leaseID, &need, &singleton, (*pq.StringArray)(&waits), &available, &expires); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, annotateError(err)
	}

	lease := &Lease{
		jobID:        JobID(jobID),
		leaseID:      leaseID,
		worker:       worker,
		capability:   Capability(need),
		leaseExpires: expires,
	}
	return lease, nil
}

// AwaitWork polls pgwf.get_work with exponential backoff until a lease is found or the context ends.
func AwaitWork(ctx context.Context, db DB, worker WorkerID, caps []Capability) (*Lease, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	backoff := initialBackoff
	for {
		lease, err := GetWork(ctx, db, worker, caps)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < maxBackoffInterval {
			backoff *= 2
			if backoff > maxBackoffInterval {
				backoff = maxBackoffInterval
			}
		}
	}
}

func capabilitiesToStrings(caps []Capability) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if c == "" {
			continue
		}
		out = append(out, string(c))
	}
	return out
}
