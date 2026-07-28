import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { createManagementClient } from "./client";

const server = setupServer(
  http.get("http://sparerunner.test/api/v1/overview", () =>
    HttpResponse.json({
      version: "dev",
      controllerEpoch: "1",
      configuredCapacity: 2,
      activeRuns: 0,
      nodeCount: 1,
      targetCount: 0,
      conditions: [],
    }),
  ),
  http.post("http://sparerunner.test/api/v1/browser-handoffs", async ({ request }) => {
    // Asserting the request shape here is the point of this handler: it proves
    // the client sends the claim digest, which no response assertion can show.
    // oxlint-disable-next-line vitest/no-standalone-expect
    expect(await request.json()).toEqual({ claimDigest: "digest" });
    return HttpResponse.json({
      code: "TWA-DEVICE-CODE",
      state: "pending",
      expiresAt: "2026-07-27T00:10:00Z",
    });
  }),
);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("createManagementClient", () => {
  it("uses the generated-safe overview projection", async () => {
    const client = createManagementClient("http://sparerunner.test/api/v1");
    await expect(client.getOverview()).resolves.toMatchObject({
      configuredCapacity: 2,
      nodeCount: 1,
    });
  });

  it("registers only a browser handoff digest", async () => {
    const client = createManagementClient("http://sparerunner.test/api/v1");
    await expect(client.createBrowserHandoff("digest")).resolves.toEqual({
      code: "TWA-DEVICE-CODE",
      state: "pending",
      expiresAt: "2026-07-27T00:10:00Z",
    });
  });

  it("keeps GitHub setup mutations on the authenticated API contract", async () => {
    server.use(
      http.post("http://sparerunner.test/api/v1/github/app/manifest", async ({ request }) => {
        expect(request.headers.get("X-SpareRunner-CSRF")).toBe("csrf");
        expect(await request.json()).toEqual({ registrationAccount: "acme" });
        return HttpResponse.json({
          actionUrl: "https://github.com/settings/apps/new",
          manifest: '{"name":"SpareRunner"}',
          state: "twm1_test",
          expiresAt: "2026-07-27T00:10:00Z",
        });
      }),
      http.get("http://sparerunner.test/api/v1/github/installations", () =>
        HttpResponse.json({
          installations: [
            {
              id: "42",
              accountLogin: "acme",
              accountType: "Organization",
              repositorySelection: "all",
            },
          ],
        }),
      ),
      http.post("http://sparerunner.test/api/v1/github/targets", async ({ request }) => {
        expect(request.headers.get("If-Match")).toBe('"cfg-0"');
        expect(request.headers.get("X-SpareRunner-CSRF")).toBe("csrf");
        expect(await request.json()).toMatchObject({
          installationId: "42",
          scopeKind: "repository",
        });
        return HttpResponse.json({
          target: {
            id: "target-test",
            installationId: "42",
            scopeKind: "repository",
            scope: "acme/private",
            scaleSetName: "sparerunner",
            runnerProfileId: "profile-sparerunner",
            status: "ready",
            freshness: { state: "unknown" },
          },
          configurationRevision: "1",
        });
      }),
    );
    const client = createManagementClient("http://sparerunner.test/api/v1");
    await expect(client.startGitHubAppManifest?.("acme", "csrf")).resolves.toMatchObject({
      state: "twm1_test",
    });
    await expect(client.listGitHubInstallations?.()).resolves.toMatchObject({
      installations: [{ id: "42" }],
    });
    await expect(
      client.createGitHubTarget?.(
        {
          installationId: "42",
          scopeKind: "repository",
          scope: "acme/private",
          scaleSetName: "sparerunner",
          runnerProfileId: "profile-sparerunner",
        },
        "0",
        "csrf",
      ),
    ).resolves.toMatchObject({ configurationRevision: "1" });
  });

  it("posts the exact code and browser-held secret and preserves a 202 pending state", async () => {
    server.use(
      http.post("http://sparerunner.test/api/v1/browser-handoffs/claim", async ({ request }) => {
        expect(request.credentials).toBe("same-origin");
        expect(await request.json()).toEqual({
          code: "twh1.test-code",
          claimSecret: "browser-only-secret",
        });
        return HttpResponse.json(
          { state: "pending", expiresAt: "2026-07-27T00:10:00Z" },
          { status: 202 },
        );
      }),
    );
    const client = createManagementClient("http://sparerunner.test/api/v1");

    await expect(
      client.claimBrowserHandoff("twh1.test-code", "browser-only-secret"),
    ).resolves.toEqual({ status: "pending" });
  });

  for (const [status, code] of [
    [401, "browser_handoff_invalid"],
    [410, "browser_handoff_expired"],
  ] as const) {
    it(`fails closed on a ${status} browser claim rejection`, async () => {
      server.use(
        http.post("http://sparerunner.test/api/v1/browser-handoffs/claim", () =>
          HttpResponse.json(
            {
              type: "about:blank",
              title: "Browser handoff rejected",
              status,
              code,
              detail: "Create and authorize a new browser handoff.",
              instance: "/api/v1/browser-handoffs/claim",
              requestId: "req_0123456789abcdef0123456789abcdef",
            },
            { status, headers: { "Content-Type": "application/problem+json" } },
          ),
        ),
      );
      const client = createManagementClient("http://sparerunner.test/api/v1");

      await expect(client.claimBrowserHandoff("code", "secret")).rejects.toMatchObject({
        name: "ApiError",
        status,
        problem: { code },
      });
    });
  }

  it("streams invalidations with the session CSRF header and an opaque resume cursor", async () => {
    server.use(
      http.get("http://sparerunner.test/api/v1/events", ({ request }) => {
        expect(request.credentials).toBe("same-origin");
        expect(request.headers.get("X-SpareRunner-CSRF")).toBe("csrf-for-stream");
        return new HttpResponse(
          [
            "id: 3:7:1",
            "event: invalidate",
            'data: {"schemaVersion":1,"cursor":"3:7:1","resources":["nodes","audit_events"]}',
            "",
            "",
          ].join("\n"),
          { headers: { "Content-Type": "text/event-stream" } },
        );
      }),
    );
    const client = createManagementClient("http://sparerunner.test/api/v1");
    let stop: () => void = () => undefined;
    const received = new Promise<unknown>((resolve) => {
      stop = client.subscribe("csrf-for-stream", undefined, {
        onEvent: resolve,
        onError: () => undefined,
      });
    });

    await expect(received).resolves.toEqual({
      kind: "invalidate",
      cursor: "3:7:1",
      resources: ["nodes", "audit_events"],
    });
    stop();
  });

  it("rejects a stream frame whose event ID and payload cursor disagree", async () => {
    server.use(
      http.get(
        "http://sparerunner.test/api/v1/events",
        () =>
          new HttpResponse(
            [
              "id: 3:7:1",
              "event: invalidate",
              'data: {"schemaVersion":1,"cursor":"3:7:2","resources":["nodes"]}',
              "",
              "",
            ].join("\n"),
            { headers: { "Content-Type": "text/event-stream" } },
          ),
      ),
    );
    const client = createManagementClient("http://sparerunner.test/api/v1");
    const onError = vi.fn();
    let stop: () => void = () => undefined;
    const rejected = new Promise<void>((resolve) => {
      stop = client.subscribe("csrf-for-stream", undefined, {
        onEvent: () => undefined,
        onError: (failure) => {
          onError(failure);
          resolve();
        },
      });
    });

    await rejected;
    stop();
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith({ kind: "transient" });
  });

  it("stops reconnecting when the event stream loses session authority", async () => {
    const requestCounts = new Map<number, number>();
    server.use(
      ...([401, 403] as const).map((status) =>
        http.get(`http://sparerunner.test/api/${status}/events`, () => {
          requestCounts.set(status, (requestCounts.get(status) ?? 0) + 1);
          return new HttpResponse(null, { status });
        }),
      ),
    );

    const stops: Array<() => void> = [];
    const failures = ([401, 403] as const).map(
      (status) =>
        new Promise<void>((resolve) => {
          const client = createManagementClient(`http://sparerunner.test/api/${status}`);
          stops.push(
            client.subscribe("csrf-for-stream", undefined, {
              onEvent: () => undefined,
              onError: (failure) => {
                expect(failure).toEqual({ kind: "authentication", status });
                resolve();
              },
            }),
          );
        }),
    );

    await Promise.all(failures);
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    expect(requestCounts).toEqual(
      new Map<number, number>([
        [401, 1],
        [403, 1],
      ]),
    );
    for (const stop of stops) stop();
  });
});
