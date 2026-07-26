# GitHub scale-set sandbox contract

Status: local fake-contract coverage is complete; live GitHub sandbox acceptance is pending.

`internal/github` is the only package allowed to import
`github.com/actions/scaleset` (pinned to v0.4.0). The controller creates one
adapter client per GitHub App installation; it does not share authentication,
message sessions, or target state between installations.

## Local contract already exercised

`go test ./test/contract/github ./internal/github` uses fakes to prove the
controller-facing order below:

1. Long-poll the session with the **last acknowledged** message ID and current
   backed capacity.
2. Deduplicate `(scaleSetID, messageID)` and atomically commit the message,
   reservation, and desired execution in the durable handler.
3. Call GitHub `DeleteMessage` only after that commit succeeds.
4. Advance the poll cursor only after `DeleteMessage` succeeds.

Thus a durable-handler failure produces no acknowledgement. An acknowledgement
failure leaves the cursor unchanged, so GitHub redelivery invokes the idempotent
handler again instead of losing the job or creating another desired execution.
The adapter does not use the upstream high-level listener because its current
implementation acknowledges before calling its scaler callback.

The fake-contract suite also verifies JIT and GitHub App key canaries are absent
from `fmt` formatting and JSON serialization. The encoded JIT body is accessible
only through `JITConfig.Deliver`; persistence may record `JITConfig.Digest()` and
delivery state, never the body.

## Required live private-sandbox acceptance

This must run only against an administrator-owned, private GitHub sandbox. Do
not place App keys, installation tokens, or JIT values in `.env`, command-line
arguments, test fixtures, logs, or uploaded artifacts. Obtain credentials from
the configured OS/managed credential store and inject them directly into the
controller process through its future secret-store integration.

Record the following in an access-controlled release-evidence location:

1. Create a dedicated GitHub App from the manifest flow and install it into two
   private sandbox scopes. Record installation IDs and non-secret target IDs.
2. For each installation, create a scale set, verify its group/label policy,
   then update and delete a disposable scale set. Confirm state stays isolated
   between installations.
3. Run a private job and capture the time from GitHub assignment to runner
   registration/job pickup. Compare it with GitHub's current pickup/requeue
   behavior; this is a measured release gate, not a hard-coded timeout.
4. Kill the controller after the SQLite durable commit and before
   `DeleteMessage`. Restart it, verify redelivery reaches the same desired
   execution, and prove exactly one runner is started.
5. Generate JIT only after the selected agent is ready. Verify the JIT canary is
   absent from controller SQLite, agent journal, JSON logs, metrics, API
   responses, diagnostics, runner workspace, and any test evidence.

Until these checks are recorded against a live private sandbox, `twk-002`
remains `in_progress`; the local tests are not a claim of GitHub runtime proof.
