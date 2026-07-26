# Tewake Requirements

Status: accepted for implementation

Feature ID: `tewake`

Audience: individual developers and small trusted teams

## Goal

Tewake shall let a user join Windows, macOS, and Linux computers they already own
to one LAN-first fleet, then distribute GitHub Actions jobs from multiple private
repositories and organizations across the available computers.

The product replaces the repeated manual workflow of SSHing into each computer,
downloading and registering a runner per scope, installing a service, tuning
parallelism, and re-registering broken runners.

## Actors

- **Administrator**: the single person who owns the controller, enrolled computers,
  and GitHub App installations.
- **Node owner**: the trusted administrator of an enrolled computer. For the first
  release this is the same trust domain as the Administrator.
- **GitHub**: the external source of job assignments, scale-set state, runner
  registration, and JIT runner configuration.
- **Agent**: the privileged local supervisor that reports node capacity and owns the
  runner process lifecycle.
- **Workflow**: code executed by the official GitHub Actions runner. Native workflows
  are trusted code, but they remain untrusted input to Tewake's protocol and file
  handling.

## User Stories

- As an Administrator, I want to initialize and serve one controller with two
  commands so that I do not need a separate database or web server.
- As an Administrator, I want to join another computer with one short-lived code so
  that I do not need to copy certificates, open an inbound port on the node, or
  manually register a GitHub runner.
- As an Administrator, I want to connect one GitHub App to multiple accounts and
  organizations so that one fleet can serve several private scopes.
- As a workflow author, I want to use `runs-on: tewake` for platform-neutral work
  and `tewake-linux`, `tewake-macos`, or `tewake-windows` for platform-specific
  work.
- As an Administrator, I want node and fleet concurrency limits to be enforced so
  that background CI does not overwhelm a personal computer.
- As an Administrator, I want explicit offline, stale, draining, and quarantined
  states so that missing data is never presented as a healthy empty fleet.
- As an Administrator, I want a one-job runner identity and disposable workspace so
  that jobs do not leave ordinary runner state behind.

## Primary Journeys

### Initialize a controller

1. Run `tewake init`.
2. Tewake creates the controller identity, SQLite database, single-admin session,
   and first one-time join code.
3. Run `tewake serve`, or install the controller as an OS service with
   `tewake install controller`.
4. Open the loopback-only Web UI.

### Join a node

1. Run `tewake join twk_...` on the node.
2. The CLI discovers endpoint candidates with mDNS or uses address hints embedded in
   the code.
3. The CLI verifies the pinned controller fingerprint before sending the one-time
   secret.
4. The agent generates its private key locally, obtains a node certificate, installs
   its OS service, and creates an outbound mTLS WebSocket session.
5. The node appears in the Web UI with its measured OS, architecture, CPU, memory,
   package cache, and slot state.

### Connect GitHub

1. Start the GitHub App Manifest flow from the Web UI.
2. Create an App owned by the user and install it into one or more accounts or
   organizations.
3. Create a GitHub Target for a private repository or organization scope.
4. Reject public scopes, unverifiable visibility, unsafe runner-group access, and
   overlapping repository-/organization-level routing.

### Run a job

1. GitHub assigns demand to a configured scale set.
2. The controller reserves one concrete node slot for that target.
3. The controller persists the message and desired execution before acknowledging
   the message.
4. The agent prepares the runner package and runtime directory.
5. The controller generates JIT configuration last and delivers it to the selected
   node without writing the body to Controller or Agent persistence.
6. The official runner executes exactly one job.
7. The agent destroys the process tree, runtime directory, workspace, and every
   configuration or credential file materialized by the official runner before
   releasing the slot.

## Exception Journeys

- If GitHub returns a transient error, the system retains the last-known state,
  marks it stale, and creates no new desired execution from unknown state.
- If an agent disconnects while a job is running, the local job continues and the
  agent completes cleanup from its local journal; state is reconciled after
  reconnect.
- If cleanup cannot be proven complete, the node becomes quarantined and advertises
  no new capacity.
- If the controller restarts after committing a message but before acknowledging it,
  message replay resolves to the same desired execution and does not start a second
  runner.
- If a node certificate, join code, controller fingerprint, protocol version, or
  expected execution state is invalid, the operation fails closed with an explicit
  error and audit event.

## Constraints

- The first tagged release supports controller and agent binaries on Linux, macOS,
  and Windows, on supported `amd64` and `arm64` combinations.
- GitHub.com is the only forge. GHES, Gitea, and enterprise-level runners are out of
  scope.
- The official `actions/scaleset` Go client is isolated behind an internal adapter.
  The official `actions/runner` binary executes jobs.
- Native JIT ephemeral runner mode is the default and is only for private repositories
  with trusted workflows, contributors, actions, and dependencies.
- Native ephemeral means a one-job GitHub runner identity and disposable work
  directory; it does not mean a disposable machine or security sandbox.
- The controller is a single process with SQLite. Controller high availability is
  not a first-release requirement.
- Nodes make outbound WSS connections authenticated with node certificates. mDNS is
  endpoint discovery only and is never an identity or authorization signal.
- The controller CA defaults to a ten-year validity. Controller and node leaf
  certificates default to one year and automatically renew at a jittered point
  between 70% and 90% of their lifetime. Expired or superseded leaf credentials fail
  closed.
- Mutable configuration is stored in SQLite. Versioned YAML import/export may
  contain non-secret settings only.
- The management listener is loopback-only by default. LAN exposure requires an
  explicit certificate or authenticated reverse proxy configuration.
- The default node `maxRunners` is 1. A fleet-wide maximum is optional and otherwise
  equals the sum of node maxima.
- Tewake shall not invent a node-count quota or other product limit without a
  measured resource boundary or platform contract.

## Acceptance

### Enrollment and platform support

- When fresh Linux, macOS, and Windows nodes receive valid join codes, each node
  shall enroll into the same controller and reconnect after an OS service restart.
- If two agents race to consume the same join code, exactly one shall succeed.
- If discovery returns a controller whose fingerprint does not match the code, the
  agent shall send no join secret.
- If a revoked node reconnects with an old certificate, the controller shall reject
  it and advertise no capacity for that node.
- A connected node shall renew its leaf certificate before expiry without changing
  its Node ID; a superseded serial and an expired certificate shall both be rejected.

### GitHub and routing

- When two or more GitHub App installations are configured, the controller shall
  maintain independent target and scale-set state for each installation.
- When a target scope is public, visibility is unknown, or runner-group safety
  cannot be verified, target creation shall fail.
- When repository- and organization-level targets would route the same label for the
  same repository, configuration shall fail with an actionable conflict.
- When a workflow requests `tewake-linux`, `tewake-macos`, or `tewake-windows`, only
  nodes matching that OS and architecture profile shall be eligible.

### Capacity and lifecycle

- For every execution history, active plus reserved slots shall never exceed either
  `node.maxRunners` or the configured fleet maximum.
- A free physical slot shall be granted to at most one GitHub Target at a time.
- Duplicate commands and duplicate scale-set messages shall resolve idempotently and
  shall not create a second runtime.
- After a successful job, runner registration, workspace, runtime process tree,
  execution-specific runner directory, and the configuration/credential files
  materialized from JIT configuration shall be absent.
- If cleanup verification fails, the execution shall enter `CleanupFailed` and the
  node shall enter `Quarantined` rather than `Idle`.
- A warm node shall start the official runner quickly enough to satisfy GitHub's
  60-second job-pickup window; measured startup latency is a release gate rather than
  a hidden runtime timeout.

### Recovery and degraded state

- If the controller stops after desired-state commit and before GitHub message
  acknowledgement, replay after restart shall not lose the job or start it twice.
- If an agent disconnects during a running job, the job shall continue locally and
  cleanup shall be journaled for later reconciliation.
- If GitHub returns a 5xx response, the UI and API shall retain last-known data and
  mark it stale instead of returning an empty healthy collection.
- After a controller restart, capacity shall begin at zero and return independently
  for each reconciled online node without waiting for offline nodes.

### Secret handling and management security

- GitHub App private keys, join secrets, JIT configuration, node private keys,
  authorization headers, and session secrets shall not appear in SQLite, YAML
  exports, ordinary logs, metrics, UI responses, or diagnostic bundles. The official
  runner's required transient `--jitconfig` argument and files under its private,
  execution-specific root are treated as secret-bearing runtime material and must
  disappear during verified cleanup.
- Unauthenticated API mutations shall return 401; authenticated cross-origin
  mutations without a valid CSRF token and Origin shall return 403.
- A missing or inaccessible OS credential store shall stop new runner admission and
  surface a degraded controller state.

## Non-Goals

- Kubernetes compatibility or a generic container orchestrator
- Cloud-provider instance provisioning
- Controller high availability
- Enterprise RBAC or multi-tenant administration
- Dominant Resource Fairness, weighted scheduling, or GPU scheduling
- GitOps CRDs or external runtime plugins
- Public repository or external-fork execution
- A claim that native or Docker mode safely sandboxes hostile code
- VM runners in the first release
- Iroh, WAN/NAT traversal, or public relay dependency
- Docker mode before the native runner core is complete
- GHES, Gitea, or enterprise-level runner scope

## Release Preconditions

- Cross-platform and multi-installation acceptance is demonstrated with real private
  GitHub sandbox scopes, not mock E2E.
- Security and failure-injection results are machine-readable and published with the
  release evidence.
- README, contributing guide, code of conduct, security policy, threat model, support
  matrix, and native-isolation limitations are present.
- Release artifacts include checksums, SBOM, and provenance. Stable macOS and Windows
  distribution additionally depends on Developer ID/notarization and Authenticode
  signing availability.
