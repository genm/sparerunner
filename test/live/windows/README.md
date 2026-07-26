# Windows native-runner acceptance

This directory captures TWK-009 host evidence. It does not replace the
three-OS/two-installation private GitHub gate owned by TWK-014.

Run only on a trusted, elevated Windows host and a clean protected commit.
Native mode is not a sandbox for untrusted code.

1. Copy `config.example.json` outside the repository.
2. Set the exact clean commit, installed Agent SHA-256, installed paths, and a
   fresh evidence directory.
3. Capture the Windows-only contract tests and packaging parser result:

   ```powershell
   .\test\live\windows\run.ps1 unit C:\absolute\live-config.json
   ```

4. Capture effective SCM identities, PIDs, boot time, binary provenance, and
   protected directory ACL state:

   ```powershell
   .\test\live\windows\run.ps1 service-preflight C:\absolute\live-config.json
   ```

5. With no assigned job or `Runner.Listener`, exercise Agent-only recovery.
   The Agent PID must change and the inert runner-identity PID must remain:

   ```powershell
   .\test\live\windows\run.ps1 service-recovery `
     C:\absolute\live-config.json `
     -ConfirmNoRunningJob
   ```

6. Capture an actual reboot in two phases:

   ```powershell
   .\test\live\windows\run.ps1 reboot-before C:\absolute\live-config.json
   Restart-Computer
   # after login/elevation
   .\test\live\windows\run.ps1 reboot-after C:\absolute\live-config.json
   ```

`windows-tests.jsonl` is the native `go test -json` stream. Other evidence files
are fixed JSON objects. The harness refuses a dirty checkout, wrong commit,
wrong installed Agent digest, inherited ACL, reparse path, reused output file,
missing service, wrong service account, wrong role, or an Agent without
`--require-native-runner`.

Still required before marking TWK-009 complete:

- `amd64` and `arm64` host artifacts;
- DPAPI plaintext-canary and cross-service-identity rejection;
- actual Job Object descendant-tree termination;
- a real locked workspace producing quarantine while preserving its locator,
  followed by verified cleanup after the lock is released;
- sleep/wake and a private GitHub one-job execution with no residual JIT,
  workspace, registration, or idle listener.

Those scenarios need environment-owned Windows machines and private GitHub
authority. They must not be replaced by mocks or inferred from cross-builds.
