import type { components } from "./generated/schema";
import type { ManagementResource } from "../ui/state-registry";

export type Schema = components["schemas"];
export type Problem = Schema["Problem"];
export type BrowserHandoffDelivery = Schema["BrowserHandoff"];
export type BrowserHandoffClaim =
  | { readonly status: "pending" }
  | { readonly status: "authenticated"; readonly session: Schema["Session"] };
export type EventInvalidation = {
  readonly kind: "ready" | "invalidate" | "reset";
  readonly cursor: string;
  readonly resources: readonly ManagementResource[];
};
export type EventStreamFailure =
  | { readonly kind: "authentication"; readonly status: 401 | 403 }
  | { readonly kind: "transient" };

export class ApiError extends Error {
  readonly problem?: Problem;
  readonly status: number;

  constructor(status: number, message: string, problem?: Problem) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
  }
}

export interface ManagementClient {
  getSession(): Promise<Schema["Session"]>;
  createBrowserHandoff(claimDigest: string): Promise<BrowserHandoffDelivery>;
  claimBrowserHandoff(code: string, claimSecret: string): Promise<BrowserHandoffClaim>;
  getSetup(): Promise<Schema["Setup"]>;
  getOverview(): Promise<Schema["Overview"]>;
  listNodes(): Promise<{
    readonly nodes: Schema["Node"][];
    readonly configurationRevision: string;
  }>;
  listTargets(): Promise<{
    readonly targets: Schema["Target"][];
    readonly configurationRevision: string;
  }>;
  listRuns(): Promise<{ readonly runs: Schema["Run"][] }>;
  listAuditEvents(cursor?: string): Promise<Schema["AuditEventPage"]>;
  getConfiguration(): Promise<Schema["Configuration"]>;
  exportConfiguration(): Promise<string>;
  applyConfiguration(
    configuration: Schema["Configuration"],
    csrfToken: string,
  ): Promise<Schema["Configuration"]>;
  createJoinCode(endpointHints: string[], csrfToken: string): Promise<Schema["JoinCodeDelivery"]>;
  cancelJoinCode(tokenId: string, csrfToken: string): Promise<void>;
  setNodeState(
    nodeId: string,
    action: "drain" | "resume" | "revoke",
    revision: string,
    csrfToken: string,
  ): Promise<{ readonly node: Schema["Node"]; readonly configurationRevision: string }>;
  subscribe(
    csrfToken: string,
    cursor: string | undefined,
    handlers: {
      readonly onEvent: (event: EventInvalidation) => void;
      readonly onError: (failure: EventStreamFailure) => void;
    },
  ): () => void;
}

const jsonHeaders = { Accept: "application/json" } as const;

export function createManagementClient(basePath = "/api/v1"): ManagementClient {
  async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await fetch(`${basePath}${path}`, {
      credentials: "same-origin",
      ...init,
      headers: { ...jsonHeaders, ...init.headers },
    });
    if (!response.ok) {
      const problem = await readProblem(response);
      throw new ApiError(response.status, problem?.detail ?? response.statusText, problem);
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return (await response.json()) as T;
  }

  return {
    getSession: () => request<Schema["Session"]>("/session"),
    // The device-code handoff keeps its claim secret in the requesting tab only.
    createBrowserHandoff: (claimDigest) =>
      request<BrowserHandoffDelivery>("/browser-handoffs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ claimDigest } satisfies Schema["CreateBrowserHandoffRequest"]),
      }),
    claimBrowserHandoff: async (code, claimSecret) => {
      const response = await fetch(`${basePath}/browser-handoffs/claim`, {
        method: "POST",
        credentials: "same-origin",
        headers: { ...jsonHeaders, "Content-Type": "application/json" },
        body: JSON.stringify({ code, claimSecret } satisfies Schema["ClaimBrowserHandoffRequest"]),
      });
      if (response.status === 202) {
        const pending = (await response.json()) as Schema["BrowserHandoffPending"];
        return { status: pending.state };
      }
      if (!response.ok) {
        const problem = await readProblem(response);
        throw new ApiError(response.status, problem?.detail ?? response.statusText, problem);
      }
      return { status: "authenticated", session: (await response.json()) as Schema["Session"] };
    },
    getSetup: () => request<Schema["Setup"]>("/setup"),
    getOverview: () => request<Schema["Overview"]>("/overview"),
    listNodes: () => request<{ nodes: Schema["Node"][]; configurationRevision: string }>("/nodes"),
    listTargets: () =>
      request<{ targets: Schema["Target"][]; configurationRevision: string }>("/targets"),
    listRuns: () => request<{ runs: Schema["Run"][] }>("/runs"),
    listAuditEvents: (cursor) =>
      request<Schema["AuditEventPage"]>(
        `/audit-events${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
      ),
    getConfiguration: () => request<Schema["Configuration"]>("/configuration"),
    exportConfiguration: async () => {
      const response = await fetch(`${basePath}/configuration/export`, {
        credentials: "same-origin",
      });
      if (!response.ok) {
        const problem = await readProblem(response);
        throw new ApiError(response.status, problem?.detail ?? response.statusText, problem);
      }
      return response.text();
    },
    applyConfiguration: (configuration, csrfToken) =>
      request<Schema["Configuration"]>("/configuration", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "If-Match": `"cfg-${configuration.revision}"`,
          "X-Tewake-CSRF": csrfToken,
        },
        body: JSON.stringify(configuration),
      }),
    createJoinCode: (endpointHints, csrfToken) =>
      request<Schema["JoinCodeDelivery"]>("/join-codes", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Tewake-CSRF": csrfToken },
        body: JSON.stringify({ endpointHints }),
      }),
    cancelJoinCode: (tokenId, csrfToken) =>
      request<void>(`/join-codes/${encodeURIComponent(tokenId)}`, {
        method: "DELETE",
        headers: { "X-Tewake-CSRF": csrfToken },
      }),
    setNodeState: (nodeId, action, revision, csrfToken) =>
      request<{ node: Schema["Node"]; configurationRevision: string }>(
        `/nodes/${encodeURIComponent(nodeId)}/${action}`,
        {
          method: "POST",
          headers: { "If-Match": `"cfg-${revision}"`, "X-Tewake-CSRF": csrfToken },
        },
      ),
    subscribe: (csrfToken, cursor, handlers) => {
      const controller = new AbortController();
      void streamInvalidations(basePath, csrfToken, cursor, handlers, controller.signal);
      return () => controller.abort();
    },
  };
}

async function streamInvalidations(
  basePath: string,
  csrfToken: string,
  initialCursor: string | undefined,
  handlers: {
    readonly onEvent: (event: EventInvalidation) => void;
    readonly onError: (failure: EventStreamFailure) => void;
  },
  signal: AbortSignal,
) {
  let cursor = initialCursor;
  while (!signal.aborted) {
    try {
      const response = await fetch(
        `${basePath}/events${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
        {
          cache: "no-store",
          credentials: "same-origin",
          headers: {
            Accept: "text/event-stream",
            "X-Tewake-CSRF": csrfToken,
          },
          signal,
        },
      );
      if (!response.ok || !response.body) {
        throw new ApiError(response.status, "Management event stream is unavailable.");
      }
      await readInvalidationStream(response.body, (event) => {
        cursor = event.cursor;
        handlers.onEvent(event);
      });
      if (signal.aborted) return;
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
        // An expired session or CSRF value cannot recover on this subscription.
        // Stop before a stale protected snapshot can look current indefinitely.
        handlers.onError({ kind: "authentication", status: error.status });
        return;
      }
    }
    handlers.onError({ kind: "transient" });
    if (!(await abortableDelay(1_000, signal))) return;
  }
}

async function readInvalidationStream(
  body: ReadableStream<Uint8Array>,
  receive: (event: EventInvalidation) => void,
) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const event = decodeInvalidationFrame(frame);
      if (event) receive(event);
      boundary = buffer.indexOf("\n\n");
    }
    if (done) return;
  }
}

function decodeInvalidationFrame(frame: string): EventInvalidation | undefined {
  let kind: EventInvalidation["kind"] | undefined;
  let id = "";
  const data: string[] = [];
  for (const line of frame.split("\n")) {
    if (!line || line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    const raw = separator < 0 ? "" : line.slice(separator + 1);
    const value = raw.startsWith(" ") ? raw.slice(1) : raw;
    if (field === "event" && isInvalidationKind(value)) kind = value;
    if (field === "id") id = value;
    if (field === "data") data.push(value);
  }
  if (!kind || !id || data.length === 0) return undefined;
  const payload = JSON.parse(data.join("\n")) as {
    readonly schemaVersion?: unknown;
    readonly cursor?: unknown;
    readonly resources?: unknown;
  };
  if (
    payload.schemaVersion !== 1 ||
    payload.cursor !== id ||
    !Array.isArray(payload.resources) ||
    !payload.resources.every(isManagementResource)
  ) {
    throw new Error("Management event stream frame is invalid.");
  }
  return { kind, cursor: id, resources: payload.resources };
}

function isInvalidationKind(value: string): value is EventInvalidation["kind"] {
  return value === "ready" || value === "invalidate" || value === "reset";
}

function isManagementResource(value: unknown): value is ManagementResource {
  return (
    value === "setup" ||
    value === "overview" ||
    value === "nodes" ||
    value === "targets" ||
    value === "runs" ||
    value === "configuration" ||
    value === "audit_events"
  );
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<boolean> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve(false);
      return;
    }
    const timer = window.setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve(true);
    }, milliseconds);
    const abort = () => {
      window.clearTimeout(timer);
      resolve(false);
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}

async function readProblem(response: Response): Promise<Problem | undefined> {
  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.includes("application/problem+json")) {
    return undefined;
  }
  return (await response.json()) as Problem;
}
