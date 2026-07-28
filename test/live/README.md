# Live evidence

Live acceptance is intentionally separate from unit, integration, and
cross-build checks. A release candidate must be exercised by a trusted commit
against an administrator-owned private GitHub sandbox and real Linux, macOS,
and Windows Agents. Fork pull requests must never send untrusted code to a
personal machine.

## SPR-014 manifest

The harness writes one JSON file to a private evidence directory (normally
`output/release-evidence/spr-014.json`). The checked-in schema is implemented by
`internal/releaseevidence`; validate it with:

```bash
sprun evidence validate --file output/release-evidence/spr-014.json
```

The validator is fail-closed and requires:

- passed observations for Linux, macOS, and Windows generic and OS-specific
  routing;
- two distinct private GitHub App installations;
- public-target and unsafe-runner-group rejection;
- restart, disconnect, drain, stale-state, quarantine, and secret-canary
  scenarios;
- a zero-finding scan over database, Agent journal, logs, metrics, UI, and
  diagnostics; and
- the exact trusted commit SHA that produced the harness.

No key, token, JIT configuration, workspace content, or free-form diagnostic
payload may be placed in the manifest. A missing host, missing installation,
partial scenario, symlink, trailing JSON, unknown field, or secret marker is an
invalid manifest. The validator does not generate a passing file.

## Existing platform harnesses

The Linux harness is under [`test/live/linux`](../live/linux) and requires the
private sandbox, an installed App, and a real systemd/cgroup-v2 host. macOS and
Windows harnesses remain release work for SPR-008 and SPR-009; no local
cross-build or synthetic fixture may be reported as their live evidence.
