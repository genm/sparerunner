// Package store owns the durable controller desired state and agent journal.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	// ErrRecoveryMode means schema validation or migration did not complete. Every
	// mutation stops until an operator restores or repairs the database.
	ErrRecoveryMode        = errors.New("store is in recovery mode")
	ErrReplayMismatch      = errors.New("store replay identity mismatch")
	ErrSlotAlreadyReserved = errors.New("store slot is already reserved")
	ErrActiveExecution     = errors.New("store active execution already exists")
	ErrWrongRole           = errors.New("store database role does not match")
	ErrCorruptBackup       = errors.New("store backup failed integrity validation")
	ErrInsecurePath        = errors.New("store path is not private")
	ErrDestinationExists   = errors.New("store destination already exists")
)

// MigrationHook is test fault injection at the boundary before version recording.
// Production callers leave it nil.
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

// ControllerStore owns desired executions, reservations, and GitHub replay state.
// It deliberately has no secret-bearing API.
type ControllerStore struct{ *baseStore }

// AgentStore owns the local journal. It stores identities and allowlisted observed
// state only, never command payload material or OS-keystore values.
type AgentStore struct{ *baseStore }

// OpenController returns a nil store for fatal path/open failures. A non-nil store
// plus an ErrRecoveryMode-wrapped error is reserved for inspectable migration or
// schema recovery failures, whose mutations fail closed.
func OpenController(ctx context.Context, path string, options Options) (*ControllerStore, error) {
	base, err := open(ctx, path, "controller", controllerMigrations, options)
	if base == nil {
		return nil, err
	}
	return &ControllerStore{base}, err
}

func OpenAgent(ctx context.Context, path string, options Options) (*AgentStore, error) {
	base, err := open(ctx, path, "agent", agentMigrations, options)
	if base == nil {
		return nil, err
	}
	return &AgentStore{base}, err
}

func open(ctx context.Context, path, role string, migrations fs.FS, options Options) (*baseStore, error) {
	canonicalPath, err := prepareDatabasePath(path)
	if err != nil {
		return nil, fmt.Errorf("open %s store: %w", role, err)
	}
	dsn, err := sqliteDSN(canonicalPath, false)
	if err != nil {
		return nil, fmt.Errorf("open %s store: %w", role, err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s database: %w", role, err)
	}
	// SQLite pragmas are connection-local; one connection keeps every write under
	// the same foreign-key and timeout contract without pool configuration drift.
	db.SetMaxOpenConns(1)
	base := &baseStore{db: db, path: canonicalPath, role: role, now: options.Now}
	if base.now == nil {
		base.now = time.Now
	}
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure %s database: %w", role, err)
	}
	if err := applyMigrations(ctx, db, role, migrations, base.now, options.MigrationHook); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	if err := verifyRole(ctx, db, role); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
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

type migration struct {
	version int
	name    string
	body    []byte
}

func loadMigrations(role string, migrations fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(migrations, "migrations/"+role)
	if err != nil {
		return nil, err
	}
	loaded := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%03d_", &version); err != nil || version < 1 {
			return nil, fmt.Errorf("invalid migration name %q", entry.Name())
		}
		body, err := fs.ReadFile(migrations, "migrations/"+role+"/"+entry.Name())
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, migration{version: version, name: entry.Name(), body: body})
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].version < loaded[j].version })
	for index, migration := range loaded {
		if migration.version != index+1 {
			return nil, fmt.Errorf("migration versions must be consecutive from 1")
		}
	}
	return loaded, nil
}

func applyMigrations(ctx context.Context, db *sql.DB, role string, migrationFS fs.FS, now func() time.Time, hook MigrationHook) error {
	migrations, err := loadMigrations(role, migrationFS)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	// One transaction covers all pending versions, retaining the exact pre-upgrade
	// schema and data if any DDL, data change, or version record fails.
	defer tx.Rollback()
	applied, hasMigrationTable, err := migrationVersions(ctx, tx)
	if err != nil {
		return err
	}
	if hasMigrationTable {
		if err := validateMigrationVersions(applied, migrations); err != nil {
			return err
		}
	}
	for _, migration := range migrations[len(applied):] {
		if _, err := tx.ExecContext(ctx, string(migration.body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.name, err)
		}
		if hook != nil {
			if err := hook(role, migration.version); err != nil {
				return fmt.Errorf("migration %d injected failure: %w", migration.version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at_unix_nano) VALUES (?, ?)", migration.version, now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func migrationVersions(ctx context.Context, q queryer) ([]int, bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT version FROM schema_migrations ORDER BY version")
	if err != nil && strings.Contains(err.Error(), "no such table: schema_migrations") {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	versions := []int{}
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, true, err
		}
		versions = append(versions, version)
	}
	return versions, true, rows.Err()
}

func validateMigrationVersions(applied []int, migrations []migration) error {
	if len(applied) > len(migrations) {
		return fmt.Errorf("database schema version is newer than this binary")
	}
	for index, version := range applied {
		if version != migrations[index].version {
			return fmt.Errorf("database schema has unknown or gapped migration version %d", version)
		}
	}
	return nil
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

func sqliteDSN(path string, readOnly bool) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	uri := &url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	if readOnly {
		query := url.Values{}
		query.Set("mode", "ro")
		uri.RawQuery = query.Encode()
	}
	return uri.String(), nil
}

func prepareDatabasePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("empty database path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if err := ensurePrivateDirectory(filepath.Dir(abs)); err != nil {
		return "", err
	}
	if err := ensurePrivateDatabaseFile(abs); err != nil {
		return "", err
	}
	return abs, nil
}

func ensurePrivateDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := createPrivateDirectory(directory); err != nil {
			return err
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: database directory is not a directory", ErrInsecurePath)
	}
	// Windows ACLs are not represented by os.FileMode. The Windows build keeps
	// this no-op and relies on the service identity/ACL installer boundary.
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: directory %q grants group or world access", ErrInsecurePath, directory)
	}
	return nil
}

func createPrivateDirectory(directory string) error {
	parent := filepath.Dir(directory)
	if parent == directory {
		return fmt.Errorf("cannot create filesystem root")
	}
	parentInfo, err := os.Lstat(parent)
	if errors.Is(err, os.ErrNotExist) {
		if err := createPrivateDirectory(parent); err != nil {
			return err
		}
		parentInfo, err = os.Lstat(parent)
	}
	if err != nil {
		return err
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: database parent is not a directory", ErrInsecurePath)
	}
	// A newly created leaf is private even when its existing parent (for example
	// a sticky temporary root) is shared. Existing store directories are never
	// chmod'd; their permissions are verified by ensurePrivateDirectory instead.
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}

func ensurePrivateDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		return file.Close()
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: database path is not a regular file", ErrInsecurePath)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: database file %q grants group or world access", ErrInsecurePath, path)
	}
	return nil
}

func quoteSQLitePath(path string) string { return "'" + strings.ReplaceAll(path, "'", "''") + "'" }

func (s *baseStore) backup(ctx context.Context, destination string) (resultErr error) {
	if err := s.requireReady(); err != nil {
		return err
	}
	destination, err := prepareDestination(destination)
	if err != nil {
		return err
	}
	temporary, err := vacuumTemporary(filepath.Dir(destination), ".tewake-backup-")
	if err != nil {
		return err
	}
	defer func() {
		if resultErr != nil {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(FULL)"); err != nil {
		return fmt.Errorf("checkpoint backup: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO "+quoteSQLitePath(temporary)); err != nil {
		return fmt.Errorf("create backup: %w", err)
	}
	if err := os.Chmod(temporary, 0o600); err != nil {
		return fmt.Errorf("secure new backup file: %w", err)
	}
	if err := ensurePrivateDatabaseFile(temporary); err != nil {
		return err
	}
	if err := validateDatabase(ctx, temporary, s.role); err != nil {
		return err
	}
	return os.Rename(temporary, destination)
}

func prepareDestination(destination string) (string, error) {
	if strings.TrimSpace(destination) == "" {
		return "", errors.New("destination is empty")
	}
	abs, err := filepath.Abs(destination)
	if err != nil {
		return "", err
	}
	if err := ensurePrivateDirectory(filepath.Dir(abs)); err != nil {
		return "", err
	}
	if _, err := os.Lstat(abs); err == nil {
		return "", fmt.Errorf("%w: %q", ErrDestinationExists, abs)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return abs, nil
}

// vacuumTemporary creates then removes an unpredictable regular file in a private
// directory because VACUUM INTO requires a non-existent destination.
func vacuumTemporary(directory, prefix string) (string, error) {
	file, err := os.CreateTemp(directory, prefix)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := ensurePrivateDatabaseFile(path); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := os.Remove(path); err != nil {
		return "", err
	}
	return path, nil
}

func validateDatabase(ctx context.Context, path, role string) error {
	if err := verifyExistingPrivateDatabaseFile(path); err != nil {
		return err
	}
	dsn, err := sqliteDSN(path, true)
	if err != nil {
		return err
	}
	db, err := sql.Open(sqliteDriver, dsn)
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
	if err := validateMigrationSchema(ctx, db, role); err != nil {
		return err
	}
	return validateColumnAllowlist(ctx, db, role)
}

func verifyExistingPrivateDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: database path is not a regular file", ErrInsecurePath)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: database file %q grants group or world access", ErrInsecurePath, path)
	}
	return nil
}

func validateMigrationSchema(ctx context.Context, db *sql.DB, role string) error {
	migrations, err := loadMigrations(role, migrationFS(role))
	if err != nil {
		return err
	}
	versions, exists, err := migrationVersions(ctx, db)
	if err != nil || !exists {
		return fmt.Errorf("%w: schema migrations unavailable: %v", ErrCorruptBackup, err)
	}
	if err := validateMigrationVersions(versions, migrations); err != nil {
		return fmt.Errorf("%w: schema migration set is not current: %w", ErrCorruptBackup, err)
	}
	if len(versions) != len(migrations) {
		return fmt.Errorf("%w: schema migration set is not current", ErrCorruptBackup)
	}
	return nil
}

func migrationFS(role string) fs.FS {
	if role == "controller" {
		return controllerMigrations
	}
	return agentMigrations
}

func validateColumnAllowlist(ctx context.Context, db *sql.DB, role string) error {
	expected := map[string][]string{
		"store_metadata":         {"key", "value"},
		"schema_migrations":      {"version", "applied_at_unix_nano"},
		"command_replays":        {"command_id", "controller_epoch", "execution_id", "expected_state", "payload_digest"},
		"execution_observations": {"execution_id", "state", "observed_at_unix_nano"},
		"cleanup_tombstones":     {"execution_id", "failure_code", "recorded_at_unix_nano"},
		"slot_reservations":      {"node_id", "slot_index", "target_id", "execution_id"},
		"executions":             {"id", "target_id", "node_id", "slot_index", "state", "created_at_unix_nano"},
		"processed_messages":     {"scale_set_id", "message_id", "message_digest", "execution_id", "created_at_unix_nano"},
	}
	tables := []string{"store_metadata", "schema_migrations"}
	if role == "controller" {
		tables = append(tables, "slot_reservations", "executions", "processed_messages")
	} else {
		tables = append(tables, "command_replays", "execution_observations", "cleanup_tombstones")
	}
	for _, table := range tables {
		rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
		if err != nil {
			return fmt.Errorf("%w: inspect %s: %v", ErrCorruptBackup, table, err)
		}
		columns := []string{}
		for rows.Next() {
			var cid int
			var name, dataType string
			var notNull, primaryKey int
			var defaultValue any
			if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
				rows.Close()
				return err
			}
			columns = append(columns, name)
		}
		if err := rows.Close(); err != nil || !sameStrings(columns, expected[table]) {
			return fmt.Errorf("%w: unexpected columns in %s", ErrCorruptBackup, table)
		}
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// Restore never overwrites a destination. Operators must stop the existing store
// and select a new path, avoiding stale WAL/sidecar replacement races.
func restore(ctx context.Context, destination, backup, role string) (resultErr error) {
	if err := validateDatabase(ctx, backup, role); err != nil {
		return err
	}
	destination, err := prepareDestination(destination)
	if err != nil {
		return err
	}
	temporaryFile, err := os.CreateTemp(filepath.Dir(destination), ".tewake-restore-")
	if err != nil {
		return err
	}
	temporary := temporaryFile.Name()
	defer func() {
		if resultErr != nil {
			_ = os.Remove(temporary)
		}
	}()
	if err := copyAndSync(temporaryFile, backup); err != nil {
		return err
	}
	if err := ensurePrivateDatabaseFile(temporary); err != nil {
		return err
	}
	if err := validateDatabase(ctx, temporary, role); err != nil {
		return err
	}
	return os.Rename(temporary, destination)
}

func copyAndSync(destination *os.File, source string) error {
	in, err := os.Open(source)
	if err != nil {
		_ = destination.Close()
		return err
	}
	defer in.Close()
	if _, err := io.Copy(destination, in); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}
