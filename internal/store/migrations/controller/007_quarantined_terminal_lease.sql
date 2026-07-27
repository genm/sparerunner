-- Quarantined is a terminal execution state. CleanupFailed retains the lease
-- while cleanup is uncertain; the later Quarantined transition atomically
-- releases that lease while node administrative quarantine keeps capacity zero.
DROP TRIGGER active_execution_reservation_guard;

-- Version 6 legitimately retained this lease. Convert that historical state
-- inside the migration before the new terminal invariant is installed.
DELETE FROM slot_reservations
WHERE execution_id IN (
    SELECT id
    FROM executions
    WHERE state = 'quarantined'
);

CREATE TRIGGER active_execution_reservation_guard
BEFORE DELETE ON slot_reservations
WHEN EXISTS (
    SELECT 1
    FROM executions
    WHERE id = OLD.execution_id
      AND state NOT IN ('released', 'failed', 'quarantined')
)
BEGIN
    SELECT RAISE(ABORT, 'active execution reservation cannot be deleted');
END;
