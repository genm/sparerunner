import type { ManagementClient, Schema } from "../api/client";
import type { ScenarioName } from "../ui/state-registry";

const baseConfiguration: Schema["Configuration"] = {
  schemaVersion: 1,
  revision: "7",
  scheduler: { maxRunners: 3 },
  nodes: [{ id: "dell-black", displayName: "dell-black", maxRunners: 2 }],
  runnerProfiles: [
    {
      id: "sparerunner",
      label: "sparerunner",
      minAvailableMemoryBytes: "0",
      versionPolicy: "auto_update",
      runtime: "native",
    },
  ],
  targets: [],
};

const baseSetup: Schema["Setup"] = {
  controllerInitialized: true,
  githubAppState: "disconnected",
  manifestFlowSupported: false,
  nodeCount: 1,
  targetCount: 0,
  conditions: [],
};

const offlineNode: Schema["Node"] = {
  id: "dell-black",
  displayName: "dell-black",
  administrativeState: "active",
  observedState: "offline",
  operatingSystem: "linux",
  architecture: "amd64",
  maxRunners: 2,
  reconciled: true,
  nativeRunnerReady: false,
  activeRunnerCount: 0,
  availableMemoryBytes: "13314398617",
  statusReason: "agent disconnected",
};

export function createScenarioClient(scenario: ScenarioName): ManagementClient {
  const unavailable = scenario === "unavailable";
  const handoffScenario =
    scenario === "handoff_required" ||
    scenario === "handoff_processing" ||
    scenario === "handoff_rejected";
  let browserClaimed = false;
  const error = () => {
    // Plain error records survive Playwright CT's cross-context prop transport.
    if (scenario === "permission_error" || (handoffScenario && !browserClaimed)) {
      throw { status: 401, message: "Administrator session required" };
    }
    if (unavailable) throw { status: 503, message: "Management authority is unavailable" };
  };
  const nodes =
    scenario === "offline" || scenario === "quarantined"
      ? [
          {
            ...offlineNode,
            administrativeState: scenario === "quarantined" ? "quarantined" : "active",
          } as Schema["Node"],
        ]
      : scenario === "stopped_by_owner"
        ? [
            {
              ...offlineNode,
              observedState: "online",
              nativeRunnerReady: true,
              statusReason: undefined,
              availabilityIntent: "stopped",
              excludedTargets: ["target-1"],
            } as Schema["Node"],
          ]
        : [];
  const runs: Schema["Run"][] =
    scenario === "running"
      ? [
          {
            id: "execution-1",
            targetId: "target-1",
            nodeId: "dell-black",
            slotIndex: 0,
            state: "running",
          },
        ]
      : [];

  return {
    async getSession() {
      if (scenario === "loading") await new Promise<never>(() => undefined);
      error();
      return { authenticated: true, csrfToken: "csrf-for-test" };
    },
    async createBrowserHandoff() {
      return { code: "TWA-TEST-DEVICE-CODE", state: "pending", expiresAt: "2026-07-27T00:10:00Z" };
    },
    async claimBrowserHandoff() {
      if (scenario === "handoff_rejected") {
        // A plain response shape survives Playwright CT's cross-context transport,
        // so this fixture remains an explicit rejection rather than an ambiguous error.
        throw { status: 410, message: "Handoff code expired" };
      }
      if (scenario === "handoff_processing") return { status: "pending" };
      browserClaimed = true;
      return {
        status: "authenticated",
        session: { authenticated: true, csrfToken: "csrf-for-test" },
      } as const;
    },
    async getSetup() {
      error();
      return baseSetup;
    },
    async getOverview() {
      error();
      return {
        version: "dev",
        controllerEpoch: "1",
        configuredCapacity: 3,
        activeRuns: runs.length,
        nodeCount: nodes.length,
        targetCount: 0,
        conditions: [],
      };
    },
    async listNodes() {
      error();
      return { nodes, configurationRevision: "7" };
    },
    async listTargets() {
      error();
      return {
        targets:
          scenario === "stale"
            ? [
                {
                  id: "target-1",
                  installationId: "installation-1",
                  scopeKind: "repository",
                  scope: "genm/sparerunner",
                  scaleSetName: "sparerunner",
                  runnerProfileId: "sparerunner",
                  status: "degraded",
                  freshness: {
                    state: "stale",
                    observedAt: "2026-07-27T00:00:00Z",
                    failureCode: "github_5xx",
                  },
                },
              ]
            : [],
        configurationRevision: "7",
      };
    },
    async listRuns() {
      error();
      return { runs };
    },
    async listAuditEvents() {
      error();
      return { events: [] };
    },
    async getConfiguration() {
      error();
      return baseConfiguration;
    },
    async exportConfiguration() {
      error();
      return "schemaVersion: 1\n";
    },
    async applyConfiguration(configuration) {
      if (scenario === "conflict") {
        throw { status: 409, message: "Configuration revision is stale" };
      }
      if (scenario === "validation_error") {
        throw {
          status: 422,
          message: "Request validation failed",
          problem: {
            type: "about:blank",
            title: "Request validation failed",
            status: 422,
            code: "validation_failed",
            detail: "Correct the listed fields and retry.",
            instance: "",
            requestId: "req-test",
            errors: [
              { field: "scheduler.maxRunners", code: "invalid", message: "must be positive" },
            ],
          },
        };
      }
      return configuration;
    },
    async createJoinCode() {
      return { tokenId: "0123456789abcdef0123456789abcdef", code: "spr_secret_for_component_test" };
    },
    async cancelJoinCode() {},
    async setNodeState(nodeId, action) {
      const node = nodes.find((candidate) => candidate.id === nodeId) ?? offlineNode;
      return {
        node: {
          ...node,
          administrativeState:
            action === "drain" ? "draining" : action === "revoke" ? "revoked" : "active",
        },
        configurationRevision: "8",
      };
    },
    subscribe() {
      return () => undefined;
    },
  };
}
