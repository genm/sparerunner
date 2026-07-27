import type { components } from "../api/generated/schema";

export type ManagementResource =
  | "setup"
  | "overview"
  | "nodes"
  | "targets"
  | "runs"
  | "configuration"
  | "audit_events";

export type ScreenName = "setup" | "overview" | "nodes" | "targets" | "runs" | "settings";

export type ScenarioName =
  | "loading"
  | "empty"
  | "running"
  | "offline"
  | "stale"
  | "permission_error"
  | "quarantined"
  | "stopped_by_owner"
  | "handoff_required"
  | "handoff_processing"
  | "handoff_rejected"
  | "unavailable"
  | "conflict"
  | "validation_error"
  | "sse_reconnecting";

export type ScreenScenario = {
  readonly name: ScenarioName;
  readonly description: string;
  readonly screen: ScreenName;
  readonly api: string;
  readonly expectation: string;
};

const scenario = (
  name: ScenarioName,
  screen: ScreenName,
  api: string,
  expectation: string,
): ScreenScenario => ({ name, screen, api, expectation, description: `${screen}: ${name}` });

// This registry is the UI state-space source of truth. Unit fixtures and Playwright
// component tests derive their state from these names rather than inventing
// independent boolean combinations.
export const screenScenarios = {
  loading: scenario(
    "loading",
    "overview",
    "initial session GET is pending",
    "Keep protected fleet data hidden while showing an explicit console-loading state.",
  ),
  empty: scenario(
    "empty",
    "nodes",
    "200 { nodes: [] }",
    "Show the normal empty state and the page's next safe action.",
  ),
  running: scenario(
    "running",
    "runs",
    "200 with an active execution",
    "Show lifecycle text and preserve the run's node and target identity.",
  ),
  offline: scenario(
    "offline",
    "nodes",
    "Node.observedState = offline",
    "Keep last-known node facts and make the offline condition explicit.",
  ),
  stale: scenario(
    "stale",
    "targets",
    "Target.freshness.state = stale",
    "Keep last-known provider data with failure and observation metadata.",
  ),
  permission_error: scenario(
    "permission_error",
    "overview",
    "401 or 403 problem response",
    "Never render protected cached data as current; explain the next owner action.",
  ),
  quarantined: scenario(
    "quarantined",
    "nodes",
    "Node administrative state or Run state is quarantined",
    "Explain capacity is unavailable and do not fabricate a release action.",
  ),
  stopped_by_owner: scenario(
    "stopped_by_owner",
    "nodes",
    "Node.availabilityIntent = stopped with an adopted excludedTargets entry",
    "Show the owner-chosen stop and per-Target exclusion distinctly from a broken runner or drain.",
  ),
  handoff_required: scenario(
    "handoff_required",
    "setup",
    "initial protected GET returns 401",
    "Static UI must ask for an owner-authorized browser handoff instead of posting a session itself.",
  ),
  handoff_processing: scenario(
    "handoff_processing",
    "setup",
    "one-time browser handoff pending",
    "Keep the short-lived code in memory only and disable navigation until the consume request settles.",
  ),
  handoff_rejected: scenario(
    "handoff_rejected",
    "setup",
    "browser handoff rejects or expires",
    "Do not expose a token; ask the owner to begin a fresh handoff.",
  ),
  unavailable: scenario(
    "unavailable",
    "overview",
    "503 management problem response",
    "Keep only confirmed last-known data and disable mutations until reload.",
  ),
  conflict: scenario(
    "conflict",
    "settings",
    "409 configuration revision conflict",
    "Keep the draft and require an explicit reload/review before retrying.",
  ),
  validation_error: scenario(
    "validation_error",
    "settings",
    "422 problem with field errors",
    "Associate each field error with its input and retain entered values.",
  ),
  sse_reconnecting: scenario(
    "sse_reconnecting",
    "overview",
    "same-origin fetch stream error",
    "Show live-update transport status without claiming provider data is stale.",
  ),
} as const satisfies Record<ScenarioName, ScreenScenario>;

export type Snapshot = {
  readonly setup: components["schemas"]["Setup"];
  readonly overview: components["schemas"]["Overview"];
  readonly nodes: components["schemas"]["Node"][];
  readonly nodesConfigurationRevision: components["schemas"]["Revision"];
  readonly targets: components["schemas"]["Target"][];
  readonly targetsConfigurationRevision: components["schemas"]["Revision"];
  readonly runs: components["schemas"]["Run"][];
  readonly configuration: components["schemas"]["Configuration"];
};

export const resourceForScreen: Readonly<Record<ScreenName, readonly ManagementResource[]>> = {
  setup: ["setup"],
  overview: ["overview", "runs", "nodes", "targets"],
  nodes: ["nodes"],
  targets: ["targets", "configuration"],
  runs: ["runs"],
  settings: ["configuration"],
};
