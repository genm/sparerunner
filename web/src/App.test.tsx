import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { ApiError } from "./api/client";
import type { EventInvalidation, ManagementClient, Schema } from "./api/client";
import { createScenarioClient } from "./test/scenario-client";

beforeAll(() => {
  // jsdom intentionally omits the modal dialog methods exercised by real-browser CT.
  Object.defineProperty(HTMLDialogElement.prototype, "showModal", {
    configurable: true,
    value(this: HTMLDialogElement) {
      this.open = true;
    },
  });
  Object.defineProperty(HTMLDialogElement.prototype, "close", {
    configurable: true,
    value(this: HTMLDialogElement) {
      this.open = false;
    },
  });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("App", () => {
  it("keeps the production API client stable across authorization-state renders", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            type: "about:blank",
            title: "Authentication required",
            status: 401,
            code: "authentication_failed",
            detail: "A valid administrator session is required.",
            instance: "urn:sparerunner:request:req_test",
            requestId: "req_test",
          }),
          {
            status: 401,
            headers: { "Content-Type": "application/problem+json" },
          },
        ),
    );
    vi.stubGlobal("fetch", fetch);

    render(<App initialRoute="overview" />);

    expect(
      await screen.findByRole("heading", { level: 1, name: "Administrator session required" }),
    ).toBeTruthy();
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 25));
    });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/session",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("keeps the node facts visible when a node is offline", async () => {
    render(<App api={createScenarioClient("offline")} initialRoute="nodes" />);

    expect(await screen.findByRole("heading", { level: 1, name: "Nodes" })).toBeTruthy();
    expect(screen.getAllByText("dell-black")).toHaveLength(2);
    expect(screen.getByText("Offline")).toBeTruthy();
    expect(screen.getByText(/last known/i)).toBeTruthy();
  });

  it("does not turn an expired owner session into an empty fleet", async () => {
    render(<App api={createScenarioClient("permission_error")} initialRoute="overview" />);

    expect(
      await screen.findByRole("heading", { level: 1, name: "Administrator session required" }),
    ).toBeTruthy();
    expect(screen.queryByText("Configured slots")).toBeNull();
  });

  it("removes a protected snapshot when the event stream loses session authority", async () => {
    const base = createScenarioClient("empty");
    let streamHandlers: Parameters<ManagementClient["subscribe"]>[2] | undefined;
    const api: ManagementClient = {
      ...base,
      subscribe: (_csrfToken, _cursor, handlers) => {
        streamHandlers = handlers;
        return () => undefined;
      },
    };
    render(<App api={api} initialRoute="overview" />);

    expect(await screen.findByRole("heading", { level: 1, name: "Overview" })).toBeTruthy();
    await waitFor(() => expect(streamHandlers).toBeDefined());
    act(() => {
      streamHandlers?.onError({ kind: "authentication", status: 401 });
    });

    expect(
      await screen.findByRole("heading", { level: 1, name: "Administrator session required" }),
    ).toBeTruthy();
    expect(screen.queryByText("Configured slots")).toBeNull();
    expect(screen.queryByRole("navigation", { name: "Management sections" })).toBeNull();
  });

  it("moves focus to the destination heading after hash navigation", async () => {
    const user = userEvent.setup();
    render(<App api={createScenarioClient("empty")} initialRoute="overview" />);

    await user.click(await screen.findByRole("link", { name: "Nodes" }));

    expect(document.activeElement).toBe(
      await screen.findByRole("heading", { level: 1, name: "Nodes" }),
    );
  });

  it("completes the two-party browser handoff before reading fleet data", async () => {
    const user = userEvent.setup();
    render(<App api={createScenarioClient("handoff_required")} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Begin browser authorization" }));
    expect(await screen.findByText(/sprun ui authorize 'TWA-TEST-DEVICE-CODE'/)).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "Continue after authorization" }));

    expect(await screen.findByRole("heading", { level: 1, name: "Setup" })).toBeTruthy();
    expect(screen.queryByText("TWA-TEST-DEVICE-CODE")).toBeNull();
  });

  it("keeps a pending claim secret out of rendered, URL, and browser storage surfaces", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("handoff_processing");
    let claimedSecret = "";
    const api: ManagementClient = {
      ...base,
      claimBrowserHandoff: vi.fn(async (_code, secret) => {
        claimedSecret = secret;
        return { status: "pending" } as const;
      }),
    };
    const view = render(<App api={api} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Begin browser authorization" }));
    await user.click(screen.getByRole("button", { name: "Continue after authorization" }));
    expect(await screen.findByText(/Authorization is still pending/i)).toBeTruthy();
    expect(claimedSecret).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(
      [
        document.body.textContent ?? "",
        window.location.href,
        storageContents(window.localStorage),
        storageContents(window.sessionStorage),
      ].join("\n"),
    ).not.toContain(claimedSecret);

    await user.click(screen.getByRole("button", { name: "Start over" }));
    expect(screen.queryByText("TWA-TEST-DEVICE-CODE")).toBeNull();
    expect(screen.queryByText(/Authorization is still pending/i)).toBeNull();

    view.unmount();
    render(<App api={createScenarioClient("handoff_processing")} initialRoute="setup" />);
    expect(await screen.findByRole("button", { name: "Begin browser authorization" })).toBeTruthy();
    expect(screen.queryByText("TWA-TEST-DEVICE-CODE")).toBeNull();
  });

  it("scrubs a rejected browser handoff and requires a fresh start", async () => {
    const user = userEvent.setup();
    render(<App api={createScenarioClient("handoff_rejected")} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Begin browser authorization" }));
    await user.click(screen.getByRole("button", { name: "Continue after authorization" }));

    expect(await screen.findByText(/rejected or expired/i)).toBeTruthy();
    expect(screen.queryByText("TWA-TEST-DEVICE-CODE")).toBeNull();
    expect(screen.getByRole("button", { name: "Begin browser authorization" })).toBeTruthy();
  });

  it("reconciles an interrupted claim response against the current session", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("empty");
    let sessionReads = 0;
    const getSession = vi.fn(async () => {
      sessionReads += 1;
      if (sessionReads === 1) throw new ApiError(401, "Administrator session required");
      return { authenticated: true, csrfToken: "csrf-for-test" } as const;
    });
    const api: ManagementClient = {
      ...base,
      getSession,
      claimBrowserHandoff: vi.fn(async () => {
        throw new SyntaxError("truncated response body");
      }),
    };
    render(<App api={api} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Begin browser authorization" }));
    await user.click(screen.getByRole("button", { name: "Continue after authorization" }));

    expect(await screen.findByRole("heading", { level: 1, name: "Setup" })).toBeTruthy();
    expect(getSession).toHaveBeenCalledTimes(2);
    expect(screen.queryByText("TWA-TEST-DEVICE-CODE")).toBeNull();
    expect(screen.queryByText(/rejected or expired/i)).toBeNull();
  });

  it("keeps an unconfirmed claim retryable when session reconciliation is unavailable", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("empty");
    const getSession = vi
      .fn<ManagementClient["getSession"]>()
      .mockRejectedValueOnce(new ApiError(401, "Administrator session required"))
      .mockRejectedValueOnce(new ApiError(503, "Controller unavailable"));
    const api: ManagementClient = {
      ...base,
      getSession,
      claimBrowserHandoff: vi.fn(async () => {
        throw new SyntaxError("truncated response body");
      }),
    };
    render(<App api={api} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Begin browser authorization" }));
    await user.click(screen.getByRole("button", { name: "Continue after authorization" }));

    expect(await screen.findByText(/authorization result could not be confirmed/i)).toBeTruthy();
    expect(screen.getByText(/TWA-TEST-DEVICE-CODE/)).toBeTruthy();
    expect(
      [
        window.location.href,
        storageContents(window.localStorage),
        storageContents(window.sessionStorage),
      ].join("\n"),
    ).not.toMatch(/[A-Za-z0-9_-]{43}/);
    expect(getSession).toHaveBeenCalledTimes(2);
  });

  it("uses the node-list revision for a drain mutation", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("offline");
    const setNodeState = vi.fn(base.setNodeState);
    const api: ManagementClient = { ...base, setNodeState };
    render(<App api={api} initialRoute="nodes" />);

    await user.click(await screen.findByRole("button", { name: "Drain" }));

    await waitFor(() =>
      expect(setNodeState).toHaveBeenCalledWith("dell-black", "drain", "7", "csrf-for-test"),
    );
    expect(await screen.findByText("dell-black is draining.")).toBeTruthy();
  });

  it("retains a configuration draft until a revision conflict is explicitly reviewed", async () => {
    const user = userEvent.setup();
    render(<App api={createScenarioClient("conflict")} initialRoute="settings" />);

    const maximum = await screen.findByRole("spinbutton", { name: "Maximum runners" });
    // fireEvent.change, not user.clear()+user.type(): jsdom does not support
    // setSelectionRange on a number input, so userEvent's select-all-then-
    // delete silently no-ops under CPU contention, and the keystrokes that
    // follow append onto the untouched original value instead of replacing
    // it. Setting the value directly is deterministic and still exercises
    // the same onChange this test is about.
    fireEvent.change(maximum, { target: { value: "2" } });
    // Confirm the edit actually landed before clicking Apply, rather than
    // trusting that fireEvent.change's synchronous return means React has
    // committed it.
    await waitFor(() => expect((maximum as HTMLInputElement).value).toBe("2"));
    await user.click(screen.getByRole("button", { name: "Apply configuration" }));

    expect(
      await screen.findByText(/Configuration changed elsewhere.*confirmed revision/i),
    ).toBeTruthy();
    expect((maximum as HTMLInputElement).value).toBe("2");
    expect(
      (screen.getByRole("button", { name: "Apply configuration" }) as HTMLButtonElement).disabled,
    ).toBe(true);

    await user.click(screen.getByRole("button", { name: "Load confirmed configuration" }));
    expect((maximum as HTMLInputElement).value).toBe("3");
  });

  it("resumes append-only activity from the final-page cursor after SSE invalidation", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("empty");
    const firstEvent: Schema["AuditEvent"] = {
      id: "audit-1",
      occurredAt: "2026-07-27T00:00:00Z",
      actor: "single_admin",
      action: "session_created",
      resourceType: "controller",
      resourceId: "",
      outcome: "succeeded",
      requestId: "req_00000000000000000000000000000001",
    };
    const secondEvent: Schema["AuditEvent"] = {
      ...firstEvent,
      id: "audit-2",
      occurredAt: "2026-07-27T00:01:00Z",
      action: "node_drained",
      resourceType: "node",
      resourceId: "dell-black",
      requestId: "req_00000000000000000000000000000002",
    };
    let sendEvent: ((event: EventInvalidation) => void) | undefined;
    const listAuditEvents = vi.fn(async (cursor?: string) =>
      cursor
        ? { events: [secondEvent], resumeCursor: "aud1_AAAAAAAAAAI" }
        : { events: [firstEvent], resumeCursor: "aud1_AAAAAAAAAAE" },
    );
    const api: ManagementClient = {
      ...base,
      listAuditEvents,
      subscribe: (_csrfToken, _cursor, handlers) => {
        sendEvent = handlers.onEvent;
        return () => undefined;
      },
    };
    render(<App api={api} initialRoute="settings" />);

    await user.click(await screen.findByRole("button", { name: "Load activity" }));
    expect(await screen.findByText("session_created")).toBeTruthy();

    await act(async () => {
      sendEvent?.({
        kind: "invalidate",
        cursor: "evt1_test",
        resources: ["audit_events"],
      });
    });

    expect(await screen.findByText("node_drained")).toBeTruthy();
    expect(listAuditEvents).toHaveBeenNthCalledWith(1, undefined);
    expect(listAuditEvents).toHaveBeenNthCalledWith(2, "aud1_AAAAAAAAAAE");
    expect(screen.queryByText(/Newer activity is available/i)).toBeNull();
  });

  it("cancels a join code whose response arrives after the dialog closes", async () => {
    const user = userEvent.setup();
    const base = createScenarioClient("empty");
    let resolveDelivery: ((delivery: Schema["JoinCodeDelivery"]) => void) | undefined;
    const pendingDelivery = new Promise<Schema["JoinCodeDelivery"]>((resolve) => {
      resolveDelivery = resolve;
    });
    const cancelJoinCode = vi.fn(async () => undefined);
    const api: ManagementClient = {
      ...base,
      createJoinCode: vi.fn(() => pendingDelivery),
      cancelJoinCode,
    };
    render(<App api={api} initialRoute="setup" />);

    await user.click(await screen.findByRole("button", { name: "Create join code" }));
    await user.click(screen.getByRole("button", { name: "Create one-time code" }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    await act(async () => {
      resolveDelivery?.({
        tokenId: "0123456789abcdef0123456789abcdef",
        code: "spr_late_secret_for_test",
      });
      await pendingDelivery;
    });

    await waitFor(() =>
      expect(cancelJoinCode).toHaveBeenCalledWith(
        "0123456789abcdef0123456789abcdef",
        "csrf-for-test",
      ),
    );
    expect(screen.queryByText("spr_late_secret_for_test")).toBeNull();
  });
});

function storageContents(storage: Storage): string {
  const entries: string[] = [];
  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index);
    if (key) entries.push(key, storage.getItem(key) ?? "");
  }
  return entries.join("\n");
}
