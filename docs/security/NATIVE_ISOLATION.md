# Native runner isolation limitations

Tewake's default runner is a native, one-job ephemeral process. This choice
keeps installation simple, preserves host caches, and avoids imposing a
container runtime on personal machines.

The Agent still creates a dedicated runtime directory, starts the runner under
its platform process boundary, and removes the runner registration, JIT
material, workspace, and descendants after completion. Cleanup is an
all-or-quarantine operation: any uncertain process, path, locked file, or
journal failure leaves the Node unavailable.

These controls are containment for trusted workflows, not a hostile-code
sandbox. They do not prevent a workflow from reading files visible to its
service account, exhausting host resources, using the network, or exploiting a
host vulnerability. Operators must use private repositories and review
contributors, actions, and dependencies before routing jobs to Tewake.

Docker or VM execution is intentionally not part of the first release and must
not be advertised as a security guarantee if introduced later.
