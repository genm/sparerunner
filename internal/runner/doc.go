// Package runner implements the in-process native runner lifecycle boundary.
//
// Its MemoryJournal is deliberately non-durable and exists for local tests and
// composition. Persisting restart-safe runner records (without JIT material) is
// deferred to agent-journal integration: a replayed process PID is not trusted as
// durable ownership, so a recreated supervisor requires reconciliation rather
// than signalling or restarting it.
package runner
