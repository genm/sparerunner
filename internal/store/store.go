// Package store owns the durable controller desired state and agent journal.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteDriver = "sqlite"

//go:embed migrations/controller/*.sql
var controllerMigrations embed.FS

//go:embed migrations/agent/*.sql
var agentMigrations embed.FS

var (
	// ErrRecoveryMode means the database did not finish a verified migration. New
	// mutations must stop until an operator restores or repairs the database.
	ErrRecoveryMode        = errors.New("store is in recovery mode")
	ErrReplayMismatch      = errors.New("store replay identity mismatch")
	ErrSlotAlreadyReserved = errors.New("store slot is already reserved")
	ErrActiveExecution     = errors.New("store active execution already exists")
	ErrWrongRole           = errors.New("store database role does not match")
	ErrCorruptBackup       = errors.New("store backup failed integrity validation")
)

// MigrationHook is intentionally test-only capable fault injection at the exact
// boundary before schema version recording. Production callers leave it nil.
type MigrationHook func(role string, version int) error

type Options struct {
	Now           func() time.Time
	MigrationHook MigrationHook
}

type baseStore struct {
	db       *sql.DB
	path     string
	role     string
	now      func() time.Time
	recovery error
}

func (s *baseStore) Close() error { return s.db.Close() }

func (s *baseStore) Ready() error {
	if s.recovery != nil {
		return fmt.Errorf("%w: %v", ErrRecoveryMode, s.recovery)
	}
	return nil
}

func (s *baseStore) requireReady() error { return s.Ready() }

// ControllerStore is the sole owner of desired executions, reservations and
// GitHub message replay state. It deliberately has no secret-bearing API.
type ControllerStore struct{ *baseStore }

// AgentStore is the local durable journal. It records command identities and
// observations, never command payload material or OS-keystore values.
type AgentStore struct{ *baseStore }

func OpenController(ctx context.Context, path string, options Options) (*ControllerStore, error) {
	base, err := open(ctx, path, "controller", controllerMigrations, options)
	return &ControllerStore{base}, err
}

func OpenAgent(ctx context.Context, path string, options Options) (*AgentStore, error) {
	base, err := open(ctx, path, "agent", agentMigrations, options)
	return &AgentStore{base}, err
}

func open(ctx context.Context, path, role string, migrations embed.FS, options Options) (*baseStore, error) {
	if path == "" {
		return nil, fmt.Errorf("open %s store: empty path", role)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	db, err := sql.Open(sqliteDriver, "file:"+filepath.ToSlash(path))
	if err != nil {
		return nil, fmt.Errorf("open %s database: %w", role, err)
	}
	// SQLite pragmas are connection-local; a single connection keeps every write
	// under the same foreign-key and timeout contract without surprise pooling.
	db.SetMaxOpenConns(1)
	base := &baseStore{db: db, path: path, role: role, now: options.Now}
	if base.now == nil {
		base.now = time.Now
	}
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure %s database: %w", role, err)
	}
	if err := applyMigrations(ctx, db, role, migrations, base.now, options.MigrationHook); err != nil {
		base.recovery = err
		return base, fmt.Errorf("open %s store: %w", role, err)
	}
	if err := verifyRole(ctx, db, role); err != nil {
		base.recovery = err
		return base, fmt.Errorf("open %s store: %w", role, err)
	}
	if err := secureFile(path); err != nil {
		_ = db.Close()
		return nil, err
	}
	return base, nil
}

func configure(ctx context.Context, db *sql.DB) error {
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	return nil
}

func applyMigrations(ctx context.Context, db *sql.DB, role string, migrations embed.FS, now func() time.Time, hook MigrationHook) error {
	entries, err := migrations.ReadDir("migrations/" + role)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	// One transaction covers all pending versions, so an interrupted first open
	// retains the exact pre-migration database rather than a half-upgraded schema.
	defer tx.Rollback()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%03d_", &version); err != nil {
			return fmt.Errorf("invalid migration name %q", entry.Name())
		}
		applied, err := migrationApplied(ctx, tx, version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + role + "/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if hook != nil {
			if err := hook(role, version); err != nil {
				return fmt.Errorf("migration %d injected failure: %w", version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at_unix_nano) VALUES (?, ?)", version, now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func migrationApplied(ctx context.Context, tx *sql.Tx, version int) (bool, error) {
	var found int
	err := tx.QueryRowContext(ctx, "SELECT 1 FROM schema_migrations WHERE version = ?", version).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	// A missing migration table is normal for a brand new database.
	if err != nil && strings.Contains(err.Error(), "no such table: schema_migrations") {
		return false, nil
	}
	return err == nil, err
}

func verifyRole(ctx context.Context, db *sql.DB, role string) error {
	var actual string
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_metadata WHERE key = 'role'").Scan(&actual); err != nil {
		return err
	}
	if actual != role {
		return fmt.Errorf("%w: got %q want %q", ErrWrongRole, actual, role)
	}
	return nil
}

func secureFile(path string) error {
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil && !errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("secure store directory: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil && !errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("secure store file: %w", err)
	}
	return nil
}

func quoteSQLitePath(path string) string { return "'" + strings.ReplaceAll(path, "'", "''") + "'" }

func (s *baseStore) backup(ctx context.Context, destination string) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if destination == "" {
		return errors.New("backup destination is empty")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	temporary := filepath.Join(filepath.Dir(destination), "."+filepath.Base(destination)+".partial")
	if err := os.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(FULL)"); err != nil {
		return fmt.Errorf("checkpoint backup: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO "+quoteSQLitePath(temporary)); err != nil {
		return fmt.Errorf("create backup: %w", err)
	}
	if err := validateDatabase(ctx, temporary, s.role); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := secureFile(temporary); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return os.Rename(temporary, destination)
}

func validateDatabase(ctx context.Context, path, role string) error {
	db, err := sql.Open(sqliteDriver, "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptBackup, err)
		}
		return fmt.Errorf("%w: integrity check returned %q", ErrCorruptBackup, integrity)
	}
	if err := verifyRole(ctx, db, role); err != nil {
		return err
	}
	var latest int
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&latest); err != nil || latest != 1 {
		if err != nil {
			return fmt.Errorf("%w: schema version: %v", ErrCorruptBackup, err)
		}
		return fmt.Errorf("%w: schema version %d is unsupported", ErrCorruptBackup, latest)
	}
	return nil
}

func restore(ctx context.Context, destination, backup, role string) error {
	if err := validateDatabase(ctx, backup, role); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	temporary := filepath.Join(filepath.Dir(destination), "."+filepath.Base(destination)+".restore")
	if err := copyFile(temporary, backup); err != nil {
		return err
	}
	if err := validateDatabase(ctx, temporary, role); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := secureFile(temporary); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	// Rename is the commit point: every source check happened before destination
	// replacement, preserving an existing controller/agent DB on a bad restore.
	return os.Rename(temporary, destination)
}

func copyFile(destination, source string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
