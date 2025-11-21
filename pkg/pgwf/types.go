package pgwf

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

// JobID uniquely identifies a job in pgwf.
type JobID string

// Capability describes a worker capability.
type Capability string

// WorkerID identifies a worker process.
type WorkerID string

// DB captures the minimal subset used by pgwf helpers.
type DB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// JobDependencies define when/how a job becomes runnable.
type JobDependencies struct {
	NextNeed     Capability
	WaitFor      []JobID
	SingletonKey string
	AvailableAt  time.Time
}

func (d JobDependencies) validate() error {
	if d.NextNeed == "" {
		return fmt.Errorf("next capability is required")
	}
	return nil
}

func (d JobDependencies) waitForStrings() []string {
	if len(d.WaitFor) == 0 {
		return nil
	}
	ids := make([]string, 0, len(d.WaitFor))
	for _, id := range d.WaitFor {
		if id == "" {
			continue
		}
		ids = append(ids, string(id))
	}
	return ids
}

func (d JobDependencies) availableAtArg() any {
	if d.AvailableAt.IsZero() {
		return nil
	}
	return d.AvailableAt
}

func (d JobDependencies) singletonArg() any {
	if d.SingletonKey == "" {
		return nil
	}
	return d.SingletonKey
}

const (
	defaultLeaseSeconds   = 60
	maxBackoffInterval    = time.Minute
	initialBackoff        = time.Second
	keepAliveSafetyBuffer = 5 * time.Second
)

var (
	// ErrLeaseExpired indicates the lease can no longer be used safely.
	ErrLeaseExpired = errors.New("pgwf: lease expired")
	// ErrLeaseMismatch means the database rejected the lease_id/job_id pair.
	ErrLeaseMismatch = errors.New("pgwf: lease mismatch")
	// ErrJobNotFound indicates the job is missing from the database.
	ErrJobNotFound = errors.New("pgwf: job not found")
	// ErrDependencyViolation denotes wait_for/singleton conflicts during submission/reschedule.
	ErrDependencyViolation = errors.New("pgwf: dependency violation")
)

// Lease models a pgwf lease token.
type Lease struct {
	jobID        JobID
	leaseID      string
	worker       WorkerID
	capability   Capability
	leaseExpires time.Time

	mu               sync.RWMutex
	released         bool
	keepAliveCancel  context.CancelFunc
	keepAliveDone    chan struct{}
	keepAliveDB      *sql.DB
	keepAliveStarted bool
}

// JobID returns the job identifier associated with this lease.
func (l *Lease) JobID() JobID {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.jobID
}

// LeaseID exposes the pgwf lease identifier.
func (l *Lease) LeaseID() string {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaseID
}

// LeaseExpiry reports the local notion of when the lease expires.
func (l *Lease) LeaseExpiry() time.Time {
	if l == nil {
		return time.Time{}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaseExpires
}

func (l *Lease) isReleased() bool {
	return l == nil || l.released
}

func (l *Lease) validateActive() error {
	if l == nil {
		return fmt.Errorf("nil lease")
	}
	l.mu.RLock()
	released := l.released
	expiry := l.leaseExpires
	l.mu.RUnlock()

	if released {
		return ErrLeaseExpired
	}
	if time.Now().After(expiry) {
		return ErrLeaseExpired
	}
	return nil
}

func (l *Lease) markReleased() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.released = true
	l.mu.Unlock()
	l.stopKeepAlive()
}

func (l *Lease) updateExpiry(newExpiry time.Time) {
	l.mu.Lock()
	l.leaseExpires = newExpiry
	l.mu.Unlock()
}

func (l *Lease) startKeepAlive(db *sql.DB) {
	if l == nil || db == nil {
		return
	}
	l.mu.Lock()
	if l.keepAliveStarted {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.keepAliveCancel = cancel
	l.keepAliveDone = make(chan struct{})
	l.keepAliveDB = db
	l.keepAliveStarted = true
	l.mu.Unlock()

	go l.keepAliveLoop(ctx)
	runtime.SetFinalizer(l, func(le *Lease) {
		le.stopKeepAlive()
	})
}

func (l *Lease) stopKeepAlive() {
	l.mu.Lock()
	cancel := l.keepAliveCancel
	done := l.keepAliveDone
	started := l.keepAliveStarted
	l.keepAliveCancel = nil
	l.keepAliveDone = nil
	l.keepAliveStarted = false
	l.mu.Unlock()

	if !started {
		return
	}

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	runtime.SetFinalizer(l, nil)
}

func (l *Lease) keepAliveLoop(ctx context.Context) {
	defer close(l.keepAliveDone)
	ticker := time.NewTimer(l.nextKeepAliveInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.RLock()
			db := l.keepAliveDB
			worker := l.worker
			l.mu.RUnlock()

			if db == nil {
				return
			}

			extendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			seconds := l.secondsForExtension(time.Duration(defaultLeaseSeconds) * time.Second)
			err := l.extendInternal(extendCtx, db, seconds, worker)
			cancel()
			if err != nil {
				// stop extending if lease is gone.
				return
			}
		}
		ticker.Reset(l.nextKeepAliveInterval())
	}
}

func (l *Lease) nextKeepAliveInterval() time.Duration {
	l.mu.RLock()
	expiry := l.leaseExpires
	l.mu.RUnlock()

	remaining := time.Until(expiry)
	target := remaining/2 - keepAliveSafetyBuffer
	if target <= 0 {
		target = 5 * time.Second
	}
	return target
}

type sentinelError struct {
	marker error
	cause  error
}

func (e sentinelError) Error() string {
	if e.cause == nil {
		return e.marker.Error()
	}
	return fmt.Sprintf("%s: %v", e.marker, e.cause)
}

func (e sentinelError) Unwrap() error { return e.cause }

func (e sentinelError) Is(target error) bool {
	return target == e.marker || errors.Is(e.cause, target)
}

func wrap(marker, err error) error {
	if err == nil {
		return marker
	}
	if marker == nil {
		return err
	}
	return sentinelError{marker: marker, cause: err}
}

func annotateError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "references unknown jobs"), strings.Contains(msg, "singleton"):
		return wrap(ErrDependencyViolation, err)
	case strings.Contains(msg, "not currently leased"), strings.Contains(msg, "actively leased"), strings.Contains(msg, "active lease not found"):
		return wrap(ErrLeaseMismatch, err)
	case strings.Contains(msg, "has expired"):
		return wrap(ErrLeaseExpired, err)
	case strings.Contains(msg, "not found"):
		return wrap(ErrJobNotFound, err)
	default:
		return err
	}
}
