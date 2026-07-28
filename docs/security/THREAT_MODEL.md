# SpareRunner threat model

Status: design contract for the first release. This document is not proof that a live gate has passed.

## Scope and trust boundary

SpareRunner is a host-centric runner fleet for computers owned by the operator. The
Controller, its SQLite database, and the enrolled Agents are inside the
operator's trust boundary. GitHub is the external authority for workflow job
assignment, scale sets, runner registration, and installation scope.

The native runner executes trusted private-workflow code as a dedicated local
service account. Native execution limits accidental damage and process
ownership, but it is not a sandbox. A workflow that is intentionally malicious
or has a compromised dependency may still affect the host and any data the
service account can access.

## Assets

- GitHub App private key and installation identity.
- Controller identity, Agent private key, and enrollment material.
- One-use join-code secrets and browser-management session material.
- GitHub JIT configuration and runner registration identity.
- Workflow source, repository credentials, workspace, and package cache.
- Slot reservations, execution state, audit events, and last-known health.

## Threats and controls

| Threat | Required control | Failure behavior |
| --- | --- | --- |
| Public or untrusted workflow reaches a native runner | Reject public targets and unknown/unsafe runner groups before desired configuration commits | No Target, no scale-set capacity |
| Join code is copied or replayed | Signed code with a one-time secret hash; consume on success, cancel, or Controller restart | Enrollment rejected and audited |
| Agent connects to an impostor Controller | Fingerprint in the code plus mTLS after enrollment; mDNS is discovery only | Connection rejected |
| Controller or Agent is restarted during a command | Durable command IDs, epochs, desired state, and idempotent Agent operations | Capacity stays zero until exact reconciliation |
| JIT material is logged or persisted | Generate last, deliver once, store only a digest, scrub runtime and evidence surfaces | Execution fails closed and the Node is not released |
| Workspace or process cleanup is incomplete | Process-group/Job Object ownership and path-safe cleanup; quarantine on any failure | Slot remains unavailable |
| GitHub is unavailable | Keep existing executions and last-known provider state; do not create new desired state | UI reports `stale`; no synthetic healthy state |
| Agent disconnects during a job | Agent journal remains authoritative for local cleanup; Controller revokes native capacity | Running job may finish; reconnect performs exact snapshot reconciliation |

## Out of scope for the native first release

Public repositories, external forks, hostile contributors, multi-tenant
isolation, VM/container sandboxing, cloud-provider lifecycle, Kubernetes, and
enterprise RBAC are not made safe by this design. Docker mode, if added later,
must be documented as trusted-workflow reproducibility rather than a public-code
sandbox.

## Evidence requirement

The threat model is satisfied only when a trusted live harness produces a
`sparerunner.cross-platform-security.v1` manifest and
`sprun evidence validate --file <manifest>` accepts it. Missing, partial, or
locally mocked evidence must fail the gate; it must not be replaced with a
default `passed` value.
