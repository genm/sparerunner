# Support

SpareRunner is pre-alpha, maintained by one person, and has no supported release
and no service-level promise. What follows is where to take a question so it
reaches the right place.

## Before asking

The specification is the project source of truth, and it answers most behavior
questions directly:

- [Requirements](spec/sparerunner/requirements.md) — what the product does and
  what the first release deliberately excludes.
- [Design](spec/sparerunner/design.md) — component boundaries and ownership.
- [Task graph](spec/sparerunner/tasks.yaml) — what is done, in progress, or not
  started, with the acceptance evidence each task needs.

The README's Status section records which paths have live evidence and which are
still release gates. A path listed there as unproven is not a bug report yet.

## Where to go

| Situation | Where |
| --- | --- |
| Suspected vulnerability | [Private vulnerability reporting](https://github.com/genm/sparerunner/security/advisories/new). Never a public issue. See [SECURITY.md](SECURITY.md). |
| Something behaves incorrectly | [Bug report](https://github.com/genm/sparerunner/issues/new?template=bug_report.yml) |
| You want behavior that does not exist | [Specification change proposal](https://github.com/genm/sparerunner/issues/new?template=spec_change.yml) |
| A document is wrong, stale, or promises something the code does not do | [Documentation problem](https://github.com/genm/sparerunner/issues/new?template=docs.yml) |
| Code of Conduct concern | See [Code of Conduct enforcement](CODE_OF_CONDUCT.md#enforcement) |
| You want to contribute a change | [CONTRIBUTING.md](CONTRIBUTING.md) |

GitHub Discussions is not enabled. Questions go to an issue.

## Installing and running

There is no supported release to install. Build from source:

```bash
mise trust
just bootstrap
just build
```

[CONTRIBUTING.md](CONTRIBUTING.md) covers local setup in full, including the
macOS Keychain prompt that appears on every rebuild and how to stop it.

## What is out of scope

The first release deliberately excludes public repositories and external forks,
cloud providers, Kubernetes, controller HA, enterprise RBAC, VMs, external
plugins, WAN/NAT traversal, GPU scheduling, and Docker mode. A request for one of
those is a specification change proposal, not a bug.

SpareRunner is not a sandbox for untrusted code. Requests to make it safe for
public repositories or external fork workloads are outside the product boundary
described in [SECURITY.md](SECURITY.md).
