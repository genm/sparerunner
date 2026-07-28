import { execFile } from "node:child_process";
import { accessSync, constants } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { getPreferenceValues } from "@raycast/api";

const run = promisify(execFile);

// The extension is a client of the CLI contract, not of the local socket. That
// keeps one implementation of the local protocol, peer authorization, and
// degraded-state semantics, and it means this extension holds no controller
// credential: it can only affect the computer it runs on.
// Version 2 added the per-Target exclusion view and target attribution on
// running executions. The version is exact-matched with no pre-1.0 shim, so a
// stale extension reports an incompatibility rather than rendering a document
// it does not understand.
export const PROTOCOL_VERSION = 2;

export type Intent = "accepting" | "stopped";

export type ScopeKind = "repository" | "organization";

export interface RunningExecution {
  executionId: string;
  state: string;
  // Absent for an execution admitted before target attribution existed.
  targetId?: string;
  scope?: string;
  scopeKind?: ScopeKind;
}

export interface EligibleTarget {
  targetId: string;
  scopeKind: ScopeKind;
  scope: string;
  scaleSetName: string;
  // excluded is the controller's adopted view; locallyExcluded is this
  // computer's own durable decision; pending is their disagreement.
  excluded: boolean;
  locallyExcluded: boolean;
  pending: boolean;
}

export interface NodeStatus {
  protocolVersion: number;
  nodeId: string;
  intent: Intent;
  intentExplicit: boolean;
  intentChangedAtUnixNano: number;
  intentChangedBy: string;
  controllerConnected: boolean;
  pendingResume: boolean;
  nativeRunnerReady: boolean;
  runningExecutions: RunningExecution[] | null;
  // Omitted until the first heartbeat round trip completes.
  eligibleTargets?: EligibleTarget[];
  // Locally excluded Targets absent from the last eligible list. Excluding an
  // unknown Target is a safe no-op, rendered as not-currently-eligible.
  unknownExclusions?: string[];
  observedAtUnixNano: number;
  agentVersion: string;
}

export class NodeControlError extends Error {
  readonly errorClass: string;

  constructor(errorClass: string, message: string) {
    super(message);
    this.errorClass = errorClass;
  }
}

interface Preferences {
  cliPath?: string;
  stateDirectory?: string;
}

const STANDARD_CLI_PATHS = [
  "/opt/homebrew/bin/sprun",
  "/usr/local/bin/sprun",
  "/usr/bin/sprun",
  join(homedir(), ".local", "bin", "sparerunner"),
  join(homedir(), "go", "bin", "sparerunner"),
];

// Raycast does not ship the agent, so a missing or unusable CLI is an
// actionable installation error. It is never rendered as an assumed accepting
// or idle state.
export function resolveCLI(): string {
  const preferences = getPreferenceValues<Preferences>();
  const candidates = preferences.cliPath ? [preferences.cliPath] : STANDARD_CLI_PATHS;
  for (const candidate of candidates) {
    try {
      accessSync(candidate, constants.X_OK);
      return candidate;
    } catch {
      continue;
    }
  }
  throw new NodeControlError(
    "cli_not_found",
    preferences.cliPath
      ? `No executable SpareRunner CLI at ${preferences.cliPath}.`
      : "No executable SpareRunner CLI found. Install SpareRunner or set the CLI path in this extension's preferences.",
  );
}

function args(operation: "status" | "pause" | "resume"): string[] {
  const preferences = getPreferenceValues<Preferences>();
  const result = ["node", operation, "--json", "--source", "raycast"];
  if (preferences.stateDirectory) {
    result.push("--state-dir", preferences.stateDirectory);
  }
  return result;
}

// targetArgs builds the "node targets" invocation. Exactly one of exclude or
// include names the mutation; neither names a plain list, mirroring the CLI's
// own ambiguous-invocation refusal so this extension never sends a request
// the CLI would itself reject.
function targetArgs(action: "exclude" | "include", targetId: string): string[] {
  const preferences = getPreferenceValues<Preferences>();
  const result = ["node", "targets", `--${action}`, targetId, "--json", "--source", "raycast"];
  if (preferences.stateDirectory) {
    result.push("--state-dir", preferences.stateDirectory);
  }
  return result;
}

interface FailureDocument {
  errorClass?: string;
  message?: string;
}

export async function callNode(operation: "status" | "pause" | "resume"): Promise<NodeStatus> {
  const cli = resolveCLI();
  try {
    const { stdout } = await run(cli, args(operation), { timeout: 10_000 });
    const status = JSON.parse(stdout) as NodeStatus;
    if (status.protocolVersion !== PROTOCOL_VERSION) {
      throw new NodeControlError(
        "protocol_mismatch",
        "The installed SpareRunner CLI speaks a different node control protocol version.",
      );
    }
    return status;
  } catch (error) {
    if (error instanceof NodeControlError) {
      throw error;
    }
    throw toNodeControlError(error);
  }
}

// callNodeTarget excludes or includes one GitHub Target through the CLI, the
// same single implementation the tray and Web UI's read-only display rely on
// for this per-Target mutation. It never touches the local socket directly.
export async function callNodeTarget(action: "exclude" | "include", targetId: string): Promise<NodeStatus> {
  const cli = resolveCLI();
  try {
    const { stdout } = await run(cli, targetArgs(action, targetId), { timeout: 10_000 });
    const status = JSON.parse(stdout) as NodeStatus;
    if (status.protocolVersion !== PROTOCOL_VERSION) {
      throw new NodeControlError(
        "protocol_mismatch",
        "The installed SpareRunner CLI speaks a different node control protocol version.",
      );
    }
    return status;
  } catch (error) {
    if (error instanceof NodeControlError) {
      throw error;
    }
    throw toNodeControlError(error);
  }
}

function toNodeControlError(error: unknown): NodeControlError {
  // The CLI emits its versioned failure document on stdout; stderr carries only
  // human-readable text.
  const stdout = typeof error === "object" && error !== null ? String(Reflect.get(error, "stdout") ?? "") : "";
  if (stdout.trim().length > 0) {
    try {
      const failure = JSON.parse(stdout) as FailureDocument;
      if (failure.errorClass) {
        return new NodeControlError(failure.errorClass, failure.message ?? failure.errorClass);
      }
    } catch {
      // Fall through: an unparseable stderr is still a failure, never a state.
    }
  }
  const message = error instanceof Error ? error.message : String(error);
  return new NodeControlError("cli_failed", message);
}

// Explanations stay parallel to the tray so both surfaces present the same
// distinct states rather than one collapsing them.
export function explain(errorClass: string): string {
  switch (errorClass) {
    case "cli_not_found":
      return "Install the SpareRunner CLI, or set its path in the extension preferences.";
    case "endpoint_unavailable":
      return "The agent service is not running, or it was started without --local-control.";
    case "unauthorized_peer":
      return "This desktop account is not an authorized node owner for the agent.";
    case "protocol_mismatch":
      return "The CLI and this extension disagree on the node control protocol version.";
    case "endpoint_unsupported":
      return "Local control is unsupported on this platform.";
    case "agent_degraded":
      return "The agent could not read or record availability state.";
    default:
      return "The SpareRunner CLI could not complete the request.";
  }
}

// targetStateLabel names the same four owner-facing states the CLI's text
// renderer and the tray use: serving, adopted-excluded, an owner exclusion the
// controller has not yet adopted, and an owner inclusion it has not yet
// released. The last never renders as served, matching the additive/
// subtractive asymmetry the design specifies.
export function targetStateLabel(target: EligibleTarget): string {
  switch (true) {
    case !target.locallyExcluded && !target.excluded:
      return "serving";
    case target.locallyExcluded && target.excluded:
      return "excluded";
    case target.locallyExcluded && !target.excluded:
      return "excluded — syncing";
    default: // !target.locallyExcluded && target.excluded
      return "include pending";
  }
}

export function headline(status: NodeStatus): string {
  if (status.intent !== "accepting") {
    return "Not accepting new jobs";
  }
  if (status.pendingResume) {
    return "Resume pending — controller has not confirmed";
  }
  if (!status.nativeRunnerReady) {
    return "Accepting, but the native runner is unavailable";
  }
  return "Accepting new jobs";
}
