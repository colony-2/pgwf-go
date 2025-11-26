package pgwf

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const submitStmt = `
SELECT job_id, next_need, wait_for, payload, available_at
FROM pgwf.submit_job($1, $2, $3, $4, $5, $6, $7, $8)
`

// SubmitJob inserts workflow metadata using pgwf.submit_job.
// expiresAt is optional; zero value keeps the job leaseable indefinitely.
func SubmitJob(ctx context.Context, db DB, jobID JobID, deps JobDependencies, payload any, worker WorkerID, singletonKey string, expiresAt time.Time) error {
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
	payloadArg, err := normalizePayload(payload)
	if err != nil {
		return err
	}

	row := db.QueryRowContext(ctx, submitStmt,
		string(jobID),
		string(worker),
		string(deps.NextNeed),
		pq.Array(deps.waitForStrings()),
		payloadArg,
		singletonArg(singletonKey),
		deps.availableAtArg(),
		expiresAtArg(expiresAt),
	)

	var (
		id          string
		need        string
		waits       pq.StringArray
		payloadJSON json.RawMessage
		available   sql.NullTime
	)

	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &payloadJSON, &available); err != nil {
		return annotateError(err)
	}

	return nil
}

func expiresAtArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func singletonArg(key string) any {
	if key == "" {
		return nil
	}
	return key
}

func normalizePayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return json.RawMessage(`{}`), nil
	}
	return encodePayload(payload)
}

func normalizePayloadOverride(payload any) (json.RawMessage, error) {
	if payload == nil {
		return nil, nil
	}
	return encodePayload(payload)
}

func encodePayload(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case json.RawMessage:
		return ensurePayloadObject(v)
	case []byte:
		return ensurePayloadObject(json.RawMessage(v))
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("pgwf: encoding payload: %w", err)
		}
		return ensurePayloadObject(encoded)
	}
}

func ensurePayloadObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("pgwf: payload must be a JSON object: %w", err)
	}
	if obj == nil {
		return nil, fmt.Errorf("pgwf: payload must be a JSON object")
	}
	return raw, nil
}
