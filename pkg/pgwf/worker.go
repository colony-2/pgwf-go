package pgwf

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const getWorkStmt = `
SELECT tenant_id, job_id, lease_id, next_need, wait_for, payload, available_at, lease_expires_at
FROM pgwf.get_work($1, $2, $3, $4, $5)
`

// GetWork attempts to fetch a single job lease matching the provided capabilities.
// tenantIDs filters which tenants to serve; nil means all tenants.
func GetWork(ctx context.Context, db DB, worker WorkerID, capabilities []Capability, tenantIDs []TenantID) (*Lease, error) {
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
	tenants := tenantIDsToStrings(tenantIDs)
	row := db.QueryRowContext(ctx, getWorkStmt, string(worker), pq.Array(caps), pq.Array(tenants), defaultLeaseSeconds, 1)

	var (
		tenantID  string
		jobID     string
		leaseID   string
		need      string
		waits     pq.StringArray
		payload   json.RawMessage
		available time.Time
		expires   time.Time
	)

	if err := row.Scan(&tenantID, &jobID, &leaseID, &need, (*pq.StringArray)(&waits), &payload, &available, &expires); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, annotateError(err)
	}

	lease := &Lease{
		tenantID:     TenantID(tenantID),
		jobID:        JobID(jobID),
		leaseID:      leaseID,
		worker:       worker,
		capability:   Capability(need),
		payload:      payload,
		leaseExpires: expires,
	}
	return lease, nil
}

// AwaitWork polls pgwf.get_work with exponential backoff until a lease is found or the context ends.
// tenantIDs filters which tenants to serve; nil means all tenants.
func AwaitWork(ctx context.Context, db DB, worker WorkerID, caps []Capability, tenantIDs []TenantID) (*Lease, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pgwf: nil context")
	}
	backoff := initialBackoff
	for {
		lease, err := GetWork(ctx, db, worker, caps, tenantIDs)
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

func tenantIDsToStrings(tenantIDs []TenantID) []string {
	if tenantIDs == nil {
		return nil
	}
	out := make([]string, 0, len(tenantIDs))
	for _, t := range tenantIDs {
		if t == "" {
			continue
		}
		out = append(out, string(t))
	}
	return out
}
