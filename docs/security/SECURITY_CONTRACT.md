# SpareRunner security contract

This is the operator-visible security contract for the first release. It
defines what SpareRunner guarantees and what it deliberately does not guarantee.

## Guarantees

- Management mutations require the single local admin session, CSRF token, and
  exact same-origin checks.
- Join codes are one-use credentials. Their secret preimage is not stored;
  cancellation, successful enrollment, or Controller restart invalidates it.
- Agents generate their own key and use outbound authenticated WebSocket/mTLS.
  They do not require an inbound LAN port.
- GitHub App keys stay in the Controller credential boundary. API responses,
  YAML export, audit records, metrics, diagnostics, and ordinary logs exclude
  keys, installation tokens, join secrets, and JIT configuration.
- Target creation is private-scope only. Public repositories, unknown scopes,
  unsafe runner groups, overlapping labels, and ambiguous provider results are
  rejected before configuration is committed.
- A cleanup failure is a quarantine signal. The Node never returns to the free
  pool until an explicit reconciliation clears the failure.
- Provider failures retain last-known state and surface `stale` or `offline`;
  they are not rendered as an empty healthy fleet.

## Native execution limitations

Native ephemeral runners are intended for workflows the operator trusts. The
dedicated service account, process-group ownership, workspace deletion, and
ephemeral runner identity reduce accidental persistence but do not provide a
security boundary against malicious workflow code. Do not enroll a machine
containing secrets that the workflow account does not need, and do not route
public or unreviewed code to `runs-on: sparerunner`.

## Reporting a vulnerability

Do not include keys, tokens, JIT configuration, workspace contents, or full
diagnostics in a report. Use the private security contact published in the
repository's `SECURITY.md` once the release documentation is added. Until then,
open a local report without sensitive material and preserve the exact build
commit and scenario name.
