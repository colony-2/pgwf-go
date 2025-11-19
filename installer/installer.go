package installer

import (
    "context"
    "database/sql"
    "fmt"
    "strings"

    pgwfsql "github.com/colony-2/pgwf"
)

// Installer applies and verifies the pgwf schema.
type Installer struct {
    DB     *sql.DB
    Schema string
}

// Apply executes the embedded pgwf DDL.
func (i Installer) Apply(ctx context.Context) error {
    if i.DB == nil {
        return fmt.Errorf("pgwf: installer DB is nil")
    }
    if ctx == nil {
        return fmt.Errorf("pgwf: context is nil")
    }

    _, err := i.DB.ExecContext(ctx, i.renderedSQL())
    return err
}

// Verify checks for core pgwf tables and functions.
func (i Installer) Verify(ctx context.Context) error {
    if i.DB == nil {
        return fmt.Errorf("pgwf: installer DB is nil")
    }
    if ctx == nil {
        return fmt.Errorf("pgwf: context is nil")
    }
    schema := i.schemaName()

    for _, tbl := range []string{"jobs", "jobs_archive", "jobs_trace"} {
        if err := i.assertTable(ctx, schema, tbl); err != nil {
            return err
        }
    }
    for _, fn := range []string{"submit_job", "get_work", "extend_lease", "reschedule_job", "complete_job"} {
        if err := i.assertFunction(ctx, schema, fn); err != nil {
            return err
        }
    }
    return nil
}

func (i Installer) renderedSQL() string {
    schema := i.schemaName()
    if schema == "pgwf" {
        return pgwfsql.SQL
    }
    replacement := strings.ReplaceAll(pgwfsql.SQL, "pgwf", schema)
    return replacement
}

func (i Installer) schemaName() string {
    if i.Schema == "" {
        return "pgwf"
    }
    return i.Schema
}

func (i Installer) assertTable(ctx context.Context, schema, table string) error {
    const stmt = `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
)`
    var exists bool
    if err := i.DB.QueryRowContext(ctx, stmt, schema, table).Scan(&exists); err != nil {
        return err
    }
    if !exists {
        return fmt.Errorf("pgwf: missing table %s.%s", schema, table)
    }
    return nil
}

func (i Installer) assertFunction(ctx context.Context, schema, fn string) error {
    const stmt = `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.routines
    WHERE routine_schema = $1 AND routine_name = $2
)`
    var exists bool
    if err := i.DB.QueryRowContext(ctx, stmt, schema, fn).Scan(&exists); err != nil {
        return err
    }
    if !exists {
        return fmt.Errorf("pgwf: missing function %s.%s", schema, fn)
    }
    return nil
}
