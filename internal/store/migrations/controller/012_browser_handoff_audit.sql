-- Browser handoff authorization is a high-impact process-local mutation. Give
-- it an explicit audit action without weakening the closed actor/resource
-- checks introduced by migration 011.
DROP TRIGGER management_audit_events_no_update;
DROP TRIGGER management_audit_events_no_delete;

ALTER TABLE management_audit_events RENAME TO management_audit_events_v11;

CREATE TABLE management_audit_events (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    occurred_at_unix_nano INTEGER NOT NULL
        CHECK (occurred_at_unix_nano > 0),
    actor TEXT NOT NULL CHECK (
        actor IN ('anonymous', 'join_code', 'node', 'single_admin')
    ),
    action TEXT NOT NULL CHECK (action IN (
        'authentication_succeeded', 'authentication_failed',
        'browser_handoff_authorized',
        'enrollment_rejected', 'enrollment_unavailable',
        'agent_session_rejected',
        'session_ended', 'join_code_created', 'join_code_cancelled',
        'configuration_applied', 'node_enrolled',
        'node_drained', 'node_resumed', 'node_revoked'
    )),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'succeeded', 'rejected', 'failed'
    )),
    resource_kind TEXT NOT NULL CHECK (resource_kind IN (
        'controller', 'configuration', 'join_code', 'node'
    )),
    resource_id TEXT NOT NULL,
    error_code TEXT NOT NULL CHECK (error_code IN (
        '', 'authentication_failed', 'enrollment_rejected',
        'enrollment_rate_limited', 'enrollment_unavailable',
        'node_credential_rejected', 'agent_protocol_rejected',
        'host_rejected',
        'event_stream_rejected', 'mutation_rejected',
        'management_unavailable', 'configuration_revision_conflict',
        'validation_failed', 'join_code_not_found', 'state_conflict',
        'invalid_body', 'invalid_precondition', 'precondition_required',
        'request_forbidden', 'misdirected_host', 'method_not_allowed',
        'payload_too_large', 'unsupported_media_type',
        'session_unavailable', 'invalid_session_request'
    )),
    request_id TEXT NOT NULL CHECK (
        request_id = 'req_unavailable'
        OR (
            length(request_id) = 36
            AND substr(request_id, 1, 4) = 'req_'
            AND substr(request_id, 5) NOT GLOB '*[^0-9a-f]*'
        )
    ),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    CHECK (
        (outcome = 'succeeded' AND error_code = '')
        OR (outcome IN ('rejected', 'failed') AND error_code != '')
    ),
    CHECK (
        (
            action = 'authentication_succeeded'
            AND actor = 'single_admin'
            AND outcome = 'succeeded'
        )
        OR (
            action = 'authentication_failed'
            AND actor = 'anonymous'
            AND outcome IN ('rejected', 'failed')
        )
        OR (
            action = 'enrollment_rejected'
            AND actor = 'anonymous'
            AND outcome = 'rejected'
        )
        OR (
            action = 'enrollment_unavailable'
            AND actor = 'anonymous'
            AND outcome = 'failed'
        )
        OR (
            action = 'agent_session_rejected'
            AND actor = 'node'
            AND outcome = 'rejected'
        )
        OR (
            action = 'node_enrolled'
            AND actor = 'join_code'
            AND outcome = 'succeeded'
        )
        OR (
            action NOT IN (
                'authentication_succeeded', 'authentication_failed',
                'enrollment_rejected', 'enrollment_unavailable',
                'agent_session_rejected', 'node_enrolled'
            )
            AND actor = 'single_admin'
        )
    ),
    CHECK (
        (
            action IN (
                'authentication_succeeded', 'authentication_failed',
                'browser_handoff_authorized',
                'enrollment_rejected', 'enrollment_unavailable',
                'session_ended'
            )
            AND resource_kind = 'controller'
            AND resource_id = ''
        )
        OR (
            action = 'configuration_applied'
            AND resource_kind = 'configuration'
            AND resource_id = ''
        )
        OR (
            action IN ('join_code_created', 'join_code_cancelled')
            AND resource_kind = 'join_code'
            AND length(resource_id) > 0
            AND trim(resource_id) = resource_id
        )
        OR (
            action IN (
                'node_enrolled', 'node_drained', 'node_resumed',
                'node_revoked', 'agent_session_rejected'
            )
            AND resource_kind = 'node'
            AND length(resource_id) > 0
            AND trim(resource_id) = resource_id
        )
    )
);

INSERT INTO management_audit_events(
    sequence, occurred_at_unix_nano, actor, action, outcome, resource_kind,
    resource_id, error_code, request_id, revision
)
SELECT sequence, occurred_at_unix_nano, actor, action, outcome, resource_kind,
    resource_id, error_code, request_id, revision
FROM management_audit_events_v11;

DROP TABLE management_audit_events_v11;

CREATE TRIGGER management_audit_events_no_update
BEFORE UPDATE ON management_audit_events
BEGIN
    SELECT RAISE(ABORT, 'management audit events are append-only');
END;

CREATE TRIGGER management_audit_events_no_delete
BEFORE DELETE ON management_audit_events
BEGIN
    SELECT RAISE(ABORT, 'management audit events are append-only');
END;
