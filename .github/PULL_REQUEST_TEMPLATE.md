<!--
Read CONTRIBUTING.md before opening this pull request.
One specification task per pull request. Open it as Draft, and mark it Ready for
Review once CI on the branch is green.
-->

## Task

<!-- The single `spr-NNN` task this pull request implements, or `none` with a
     reason for repository-hygiene changes that no task owns. -->

Task: spr-NNN

## What changed

<!-- What an operator or contributor can now observe that they could not before.
     Describe behavior, not a file-by-file diff. -->

## Acceptance evidence

<!-- Which acceptance clauses of the task this proves, and how. Attach or link
     machine-readable results from output/test-results/ where relevant, and say
     plainly which clauses are NOT yet proven. -->

- Clause:
- Proven by:
- Not yet proven:

## Failure-path coverage

<!-- Every meaningful behavior change needs a normal-path test and at least one
     failure, invalid-input, timeout, permission-denied, or degraded-state test.
     Name both tests here. -->

- Normal path:
- Failure path:

## Checklist

- [ ] The specification is updated first when behavior changes: requirements, then design, then the task graph.
- [ ] `just check` passes locally.
- [ ] No new secret, credential, JIT material, or `Authorization` header can reach SQLite, logs, metrics, UI, YAML, or diagnostics.
- [ ] Security-sensitive paths fail closed, and a failure is never replaced by an empty healthy response.
- [ ] No new user-visible limit, quota, or timeout without a platform contract, demonstrated risk, or measured boundary behind it.
- [ ] Generated API and Web UI output is regenerated rather than hand-edited.
- [ ] Any new GitHub Action is pinned to a full commit SHA with the version in a trailing comment.
- [ ] Repository content is in English, and commits follow Conventional Commits with no AI or co-author trailers.
