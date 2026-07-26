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
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/genm/tewake/internal/enroll"
)

const sqliteDriver = "sqlite"

const (
	sqliteBusyTimeoutMilliseconds = 5000
	maxSQLiteInteger              = uint64(^uint64(0) >> 1)
)

//go:embed migrations/controller/*.sql
var controllerMigrations embed.FS

//go:embed migrations/agent/*.sql
var agentMigrations embed.FS

var (
	// ErrRecoveryMode means schema validation or migration did not complete. Every
	// mutation stops until an operator restores or repairs the database.
	ErrRecoveryMode         = errors.New("store is in recovery mode")
	ErrReplayMismatch       = errors.New("store replay identity mismatch")
	ErrSlotAlreadyReserved  = errors.New("store slot is already reserved")
	ErrActiveExecution      = errors.New("store active execution already exists")
	ErrWrongRole            = errors.New("store database role does not match")
	ErrCorruptBackup        = errors.New("store backup failed integrity validation")
	ErrInsecurePath         = errors.New("store path is not private")
	ErrDestinationExists    = errors.New("store destination already exists")
	ErrStaleControllerEpoch = errors.New("store command is from a stale controller epoch")
	ErrStoreBusy            = errors.New("store contention deadline exceeded")
	ErrUnownedDatabase      = errors.New("database is not owned by Tewake")
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
type ControllerStore struct {
	*baseStore
	revocationHook func(enroll.Credential)
}

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
	return &ControllerStore{baseStore: base}, err
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
	loadedMigrations, err := loadMigrations(role, migrations)
	if err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	// Both checks are read-only and deliberately precede journal_mode=WAL. An
	// unrelated SQLite database must be rejected without changing its bytes,
	// sidecars, schema, or data merely because it was passed to Tewake.
	if err := validateDatabaseCheck(ctx, db, "quick_check"); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	if _, err := validateMigrationOwnership(ctx, db, role, loadedMigrations); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure %s database: %w", role, err)
	}
	if err := applyLoadedMigrations(ctx, db, role, loadedMigrations, base.now, options.MigrationHook); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	if err := validateOpenDatabase(ctx, db, role); err != nil {
		base.recovery = err
		return base, fmt.Errorf("%w: open %s store: %w", ErrRecoveryMode, role, err)
	}
	return base, nil
}

func configure(ctx context.Context, db *sql.DB) error {
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		return fmt.Errorf("enable WAL: got journal mode %q", journalMode)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}
	var foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("verify foreign keys: %w", err)
	}
	if foreignKeys != 1 {
		return errors.New("verify foreign keys: pragma remained disabled")
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", sqliteBusyTimeoutMilliseconds)); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("verify busy timeout: %w", err)
	}
	if busyTimeout != sqliteBusyTimeoutMilliseconds {
		return fmt.Errorf("verify busy timeout: got %d", busyTimeout)
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
	return applyLoadedMigrations(ctx, db, role, migrations, now, hook)
}

func applyLoadedMigrations(ctx context.Context, db *sql.DB, role string, migrations []migration, now func() time.Time, hook MigrationHook) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	// One transaction covers all pending versions, retaining the exact pre-upgrade
	// schema and data if any DDL, data change, or version record fails.
	defer tx.Rollback()
	// Repeat ownership validation after acquiring the writer reservation. This
	// closes the read-to-write gap and ensures migrations only extend an exact
	// schema prefix previously created by this role's migration chain.
	applied, err := validateMigrationOwnership(ctx, tx, role, migrations)
	if err != nil {
		return err
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
		appliedAt, err := storeUnixNano(now())
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at_unix_nano) VALUES (?, ?)", migration.version, appliedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// validateMigrationOwnership distinguishes a genuinely empty SQLite database
// from an existing Tewake store. Owned stores must exactly match the schema and
// metadata produced by their already-applied migration prefix before any pending
// migration is allowed to run.
func validateMigrationOwnership(ctx context.Context, q queryer, role string, migrations []migration) ([]int, error) {
	applied, hasMigrationTable, err := migrationVersions(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect schema migrations: %v", ErrUnownedDatabase, err)
	}
	if !hasMigrationTable {
		objects, err := readSchemaObjects(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("%w: inspect existing schema: %v", ErrUnownedDatabase, err)
		}
		if len(objects) != 0 {
			return nil, fmt.Errorf("%w: existing database is not empty", ErrUnownedDatabase)
		}
		return nil, nil
	}
	if len(applied) == 0 {
		return nil, fmt.Errorf("%w: migration history is empty", ErrUnownedDatabase)
	}
	if err := validateMigrationVersions(applied, migrations); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptBackup, err)
	}
	if err := verifyRole(ctx, q, role); err != nil {
		return nil, err
	}
	expectedSchema, expectedMetadata, err := expectedMigrationPrefix(ctx, role, migrations, len(applied))
	if err != nil {
		return nil, fmt.Errorf("build expected %s migration prefix: %w", role, err)
	}
	if err := validateSchemaObjectSet(ctx, q, expectedSchema); err != nil {
		return nil, err
	}
	if err := validateMetadataSet(ctx, q, role, expectedMetadata); err != nil {
		return nil, err
	}
	if err := validateForeignKeys(ctx, q); err != nil {
		return nil, err
	}
	return applied, nil
}

func migrationVersions(ctx context.Context, q queryer) ([]int, bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT version, applied_at_unix_nano FROM schema_migrations ORDER BY version")
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
		var appliedAt int64
		if err := rows.Scan(&version, &appliedAt); err != nil {
			return nil, true, err
		}
		if appliedAt <= 0 {
			return nil, true, errors.New("schema migration has invalid applied timestamp")
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

func verifyRole(ctx context.Context, db queryer, role string) error {
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
	uriPath := sqliteURIPath(abs)
	// A Windows drive path must be encoded as file:///C:/..., not file://C:/....
	// The latter treats the drive letter as a URI authority and SQLite rejects it.
	uri := &url.URL{Scheme: "file", Path: uriPath}
	query := url.Values{}
	if readOnly {
		query.Set("mode", "ro")
	} else {
		// BEGIN IMMEDIATE acquires the writer reservation before replay reads.
		// Together with busy_timeout this serializes competing Store handles at
		// SQLite's ownership boundary instead of leaking a read-to-write race.
		query.Set("_txlock", "immediate")
	}
	uri.RawQuery = query.Encode()
	return uri.String(), nil
}

func sqliteURIPath(abs string) string {
	uriPath := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(uriPath, "/") {
		return "/" + uriPath
	}
	return uriPath
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
	if err := syncFile(temporary); err != nil {
		return fmt.Errorf("sync backup file: %w", err)
	}
	return publishNoReplace(temporary, destination)
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
	if err := validateIntegrity(ctx, db); err != nil {
		return err
	}
	return validateOpenDatabase(ctx, db, role)
}

func validateIntegrity(ctx context.Context, db *sql.DB) error {
	return validateDatabaseCheck(ctx, db, "integrity_check")
}

func validateDatabaseCheck(ctx context.Context, db queryer, check string) error {
	if check != "quick_check" && check != "integrity_check" {
		return fmt.Errorf("unsupported SQLite check %q", check)
	}
	rows, err := db.QueryContext(ctx, "PRAGMA "+check)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrCorruptBackup, check, err)
	}
	defer rows.Close()
	results := []string{}
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrCorruptBackup, check, err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrCorruptBackup, check, err)
	}
	if len(results) != 1 || results[0] != "ok" {
		return fmt.Errorf("%w: %s failed", ErrCorruptBackup, check)
	}
	return nil
}

func validateOpenDatabase(ctx context.Context, db *sql.DB, role string) error {
	if err := verifyRole(ctx, db, role); err != nil {
		return err
	}
	if err := validateMigrationSchema(ctx, db, role); err != nil {
		return err
	}
	if err := validateMetadata(ctx, db, role); err != nil {
		return err
	}
	if err := validateSchemaObjects(ctx, db, role); err != nil {
		return err
	}
	if err := validateForeignKeys(ctx, db); err != nil {
		return err
	}
	return validateColumnAllowlist(ctx, db, role)
}

func validateMetadata(ctx context.Context, db queryer, role string) error {
	expectedKeys := currentMetadataKeys(role)
	return validateMetadataSet(ctx, db, role, expectedKeys)
}

func currentMetadataKeys(role string) []string {
	if role == "agent" {
		return []string{"max_controller_epoch", "role"}
	}
	return []string{"controller_epoch", "role"}
}

func validateMetadataSet(ctx context.Context, db queryer, role string, expectedKeys []string) error {
	rows, err := db.QueryContext(ctx, "SELECT key FROM store_metadata ORDER BY key")
	if err != nil {
		return fmt.Errorf("%w: inspect metadata keys: %v", ErrCorruptBackup, err)
	}
	keys := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !sameStrings(keys, expectedKeys) {
		return fmt.Errorf("%w: unexpected metadata key set", ErrCorruptBackup)
	}
	epochKey := "controller_epoch"
	if role == "agent" {
		epochKey = "max_controller_epoch"
	}
	epoch, err := readUintMetadata(ctx, db, epochKey)
	if err != nil || epoch > maxSQLiteInteger {
		return fmt.Errorf("%w: invalid %s metadata", ErrCorruptBackup, epochKey)
	}
	return nil
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
	switch role {
	case "controller":
		return controllerMigrations
	case "agent":
		return agentMigrations
	default:
		return nil
	}
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
		"enrollment_tokens":      {"token_id", "secret_digest", "controller_epoch"},
		"enrolled_nodes":         {"node_id", "current_serial", "credential_epoch", "not_before_unix_nano", "not_after_unix_nano", "revoked"},
		"enrollment_replays":     {"token_id", "secret_digest", "controller_epoch", "public_key_digest", "node_id", "certificate_der", "ca_der"},
	}
	tables := []string{"store_metadata", "schema_migrations"}
	if role == "controller" {
		tables = append(tables, "slot_reservations", "executions", "processed_messages", "enrollment_tokens", "enrolled_nodes", "enrollment_replays")
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

type schemaObject struct {
	ObjectType string
	Name       string
	TableName  string
	SQL        string
}

func validateSchemaObjects(ctx context.Context, db *sql.DB, role string) error {
	expected, err := expectedSchemaObjects(ctx, role)
	if err != nil {
		return fmt.Errorf("build expected %s schema: %w", role, err)
	}
	return validateSchemaObjectSet(ctx, db, expected)
}

func validateSchemaObjectSet(ctx context.Context, db queryer, expected []schemaObject) error {
	actual, err := readSchemaObjects(ctx, db)
	if err != nil {
		return fmt.Errorf("%w: inspect schema objects: %v", ErrCorruptBackup, err)
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("%w: unexpected schema object set", ErrCorruptBackup)
	}
	for index := range actual {
		if actual[index] != expected[index] {
			return fmt.Errorf("%w: unexpected schema object definition", ErrCorruptBackup)
		}
	}
	return nil
}

func expectedSchemaObjects(ctx context.Context, role string) ([]schemaObject, error) {
	migrations := migrationFS(role)
	if migrations == nil {
		return nil, fmt.Errorf("unknown database role %q", role)
	}
	loaded, err := loadMigrations(role, migrations)
	if err != nil {
		return nil, err
	}
	schema, _, err := expectedMigrationPrefix(ctx, role, loaded, len(loaded))
	return schema, err
}

func expectedMigrationPrefix(ctx context.Context, role string, migrations []migration, appliedCount int) ([]schemaObject, []string, error) {
	if appliedCount < 1 || appliedCount > len(migrations) {
		return nil, nil, fmt.Errorf("invalid applied migration count %d", appliedCount)
	}
	db, err := sql.Open(sqliteDriver, ":memory:")
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return nil, nil, err
	}
	for _, migration := range migrations[:appliedCount] {
		if _, err := db.ExecContext(ctx, string(migration.body)); err != nil {
			return nil, nil, fmt.Errorf("apply %s: %w", migration.name, err)
		}
	}
	schema, err := readSchemaObjects(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT key FROM store_metadata ORDER BY key")
	if err != nil {
		return nil, nil, fmt.Errorf("inspect expected metadata: %w", err)
	}
	defer rows.Close()
	metadata := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, nil, err
		}
		metadata = append(metadata, key)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return schema, metadata, nil
}

func readSchemaObjects(ctx context.Context, db queryer) ([]schemaObject, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT type, name, tbl_name, COALESCE(sql, '')
		FROM sqlite_schema
		WHERE name IS NOT NULL
		ORDER BY type, name, tbl_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := []schemaObject{}
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.ObjectType, &object.Name, &object.TableName, &object.SQL); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

func validateForeignKeys(ctx context.Context, db queryer) error {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("%w: foreign key check: %v", ErrCorruptBackup, err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("%w: foreign key violation", ErrCorruptBackup)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: foreign key check: %v", ErrCorruptBackup, err)
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
	return publishNoReplace(temporary, destination)
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

func syncFile(path string) error {
	// Windows requires a writable handle for FlushFileBuffers, which backs Sync.
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// publishNoReplace atomically links a fully synced temporary file into its final
// name. Link creation fails if another writer won the destination; unlike Rename,
// it never overwrites that winner on Unix.
func publishNoReplace(temporary, destination string) error {
	if err := os.Link(temporary, destination); err != nil {
		_ = os.Remove(temporary)
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %q", ErrDestinationExists, destination)
		}
		return fmt.Errorf("publish store file: %w", err)
	}
	directory := filepath.Dir(destination)
	if err := syncDirectory(directory); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("sync published store directory: %w", err)
	}
	if err := os.Remove(temporary); err != nil {
		return fmt.Errorf("remove published temporary store: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("sync temporary removal: %w", err)
	}
	return nil
}

func readUintMetadata(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key string) (uint64, error) {
	var raw string
	if err := q.QueryRowContext(ctx, "SELECT value FROM store_metadata WHERE key = ?", key).Scan(&raw); err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid metadata %q: %w", key, err)
	}
	return value, nil
}

func storeUnixNano(moment time.Time) (int64, error) {
	minimum := time.Unix(0, 1)
	maximum := time.Unix(0, math.MaxInt64)
	if moment.Before(minimum) || moment.After(maximum) {
		return 0, errors.New("store timestamp is outside SQLite's positive signed INTEGER range")
	}
	return moment.UnixNano(), nil
}
