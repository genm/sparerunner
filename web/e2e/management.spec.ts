import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test } from "@playwright/test";

type ControllerRuntime = {
  readonly adminURL: string;
  readonly agentURL: string;
  readonly binary: string;
  readonly controllerState: string;
  readonly root: string;
  readonly serve: ChildProcessWithoutNullStreams;
};

let runtime: ControllerRuntime;

test.beforeAll(async () => {
  const configuredBinary = process.env["SPARERUNNER_E2E_BINARY"];
  if (!configuredBinary) {
    throw new Error(
      "SPARERUNNER_E2E_BINARY is required for the release-binary management journey.",
    );
  }
  const binary = resolve(configuredBinary);
  const root = await mkdtemp(join(tmpdir(), "sparerunner-management-e2e-"));
  const controllerState = join(root, "controller");
  let serve: ChildProcessWithoutNullStreams | undefined;

  try {
    await runSpareRunner(binary, ["init", "--state-dir", controllerState]);
    serve = spawn(
      binary,
      [
        "serve",
        "--state-dir",
        controllerState,
        "--agent-listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
        "--mdns=false",
      ],
      { stdio: ["pipe", "pipe", "pipe"] },
    );
    const endpoints = await waitForController(serve);
    runtime = { ...endpoints, binary, controllerState, root, serve };
  } catch (error) {
    serve?.kill("SIGTERM");
    await rm(root, { recursive: true, force: true });
    throw error;
  }
});

test.afterAll(async () => {
  if (!runtime) return;
  runtime.serve.kill("SIGTERM");
  await waitForExit(runtime.serve);
  await rm(runtime.root, { recursive: true, force: true });
});

test("authorizes the browser, manages join codes, enrolls a node, and drains it", async ({
  page,
}) => {
  let phase = "browser_open";
  const responses: { readonly path: string; readonly status: number }[] = [];
  let pageErrorCount = 0;
  page.on("response", (response) => {
    const url = new URL(response.url());
    if (url.origin === runtime.adminURL && responses.length < 64) {
      responses.push({ path: url.pathname, status: response.status() });
    }
  });
  page.on("pageerror", () => {
    pageErrorCount += 1;
  });
  try {
    await page.goto(runtime.adminURL);
    await expect(
      page.getByRole("heading", { name: "Administrator session required" }),
    ).toBeVisible();

    await page.getByRole("button", { name: "Begin browser authorization" }).click();
    const handoffCommand = await page.locator(".device-command").textContent();
    const handoffCode = exactCommandArgument(handoffCommand, "sprun ui authorize");
    phase = "browser_owner_authorization";
    await runSpareRunner(runtime.binary, [
      "ui",
      "authorize",
      handoffCode,
      "--state-dir",
      runtime.controllerState,
      "--admin-url",
      `${runtime.adminURL}/api/v1`,
    ]);
    phase = "browser_claim";
    await page.getByRole("button", { name: "Continue after authorization" }).click();
    await expect(page.getByRole("heading", { name: "Overview", exact: true })).toBeVisible();
    phase = "browser_live_updates";
    await expect(page.getByText("Live updates connected", { exact: true })).toBeVisible();
    await page.getByRole("link", { name: "Setup" }).click();

    phase = "join_code_cancellation";
    await createJoinCode(page, runtime.agentURL);
    const cancellation = page.waitForResponse(isJoinCodeDelete);
    await page.getByRole("button", { name: "Close and remove code" }).click();
    expect((await cancellation).status()).toBe(204);
    await expect(page.getByRole("dialog")).toHaveCount(0);

    phase = "node_enrollment";
    const joinCode = await createJoinCode(page, runtime.agentURL);
    const joinOutput = await runSpareRunner(runtime.binary, [
      "join",
      joinCode,
      "--state-dir",
      join(runtime.root, "agent"),
      "--controller",
      runtime.agentURL,
      "--discovery-timeout",
      "1s",
      "--connection-timeout",
      "10s",
    ]);
    const nodeID = joinedNodeID(joinOutput);
    // This background count proves SSE loaded the enrollment's new configuration
    // revision before the drain mutation reuses it as If-Match authority.
    await expect(page.getByText("1 enrolled", { exact: true })).toBeVisible();

    const consumedCancellation = page.waitForResponse(isJoinCodeDelete);
    await page.getByRole("button", { name: "Close and remove code" }).click();
    expect((await consumedCancellation).status()).toBe(404);
    await expect(page.getByRole("textbox", { name: "One-time join code" })).toHaveCount(0);
    await expect(
      page.getByText("The join code could not be cancelled.", { exact: true }),
    ).toHaveCount(0);

    phase = "node_drain";
    await page.getByRole("link", { name: "Nodes" }).click();
    const node = page.locator(".node-row").filter({ hasText: nodeID });
    await expect(node).toBeVisible();
    await node.getByRole("button", { name: "Drain" }).click();
    await expect(page.getByText(`${nodeID} is draining.`, { exact: true })).toBeVisible();
    await expect(node.getByRole("button", { name: "Resume" })).toBeVisible();

    phase = "audit_verification";
    await page.getByRole("link", { name: "Settings" }).click();
    await page.getByRole("button", { name: "Load activity" }).click();
    await expect(page.getByText("browser_handoff_authorized", { exact: true })).toBeVisible();
    await expect(page.getByText("join_code_cancelled", { exact: true })).toBeVisible();
    await expect(page.getByText("node_drained", { exact: true })).toBeVisible();
  } catch {
    // Playwright writes an automatic accessibility context for failures. Scrub
    // the credential-bearing DOM before surfacing a bounded phase-only error.
    const headings = await page
      .locator("h1")
      .allTextContents()
      .catch(() => []);
    const cookieNames = (await page.context().cookies()).map((cookie) => cookie.name);
    await page.goto("about:blank").catch(() => undefined);
    console.log(
      JSON.stringify({
        status: "failed",
        phase,
        headings,
        cookieNames,
        pageErrorCount,
        responses,
      }),
    );
    throw new Error(`Release-binary management journey failed during ${phase}.`);
  }
});

async function createJoinCode(
  page: import("@playwright/test").Page,
  agentURL: string,
): Promise<string> {
  await page.getByRole("button", { name: "Create join code" }).click();
  await page.getByRole("textbox", { name: "Endpoint hints" }).fill(agentURL);
  await page.getByRole("button", { name: "Create one-time code" }).click();
  const delivery = page.getByRole("textbox", { name: "One-time join code" });
  await expect(delivery).toBeVisible();
  const code = await delivery.inputValue();
  if (!code.startsWith("spr_")) {
    throw new Error("Controller returned a non-canonical join-code delivery.");
  }
  return code;
}

function exactCommandArgument(command: string | null, prefix: string): string {
  const match = command?.match(new RegExp(`^${prefix} '([^']+)'$`));
  if (!match?.[1]) {
    throw new Error("Browser rendered a non-canonical owner authorization command.");
  }
  return match[1];
}

function joinedNodeID(output: string): string {
  const match = output.match(/^Node ([0-9a-f]{32}) joined successfully$/m);
  if (!match?.[1]) {
    throw new Error("Join command did not confirm a canonical node identifier.");
  }
  return match[1];
}

function isJoinCodeDelete(response: import("@playwright/test").Response): boolean {
  const request = response.request();
  return (
    request.method() === "DELETE" &&
    /^\/api\/v1\/join-codes\/[0-9a-f]{32}$/.test(new URL(response.url()).pathname)
  );
}

async function runSpareRunner(binary: string, args: readonly string[]): Promise<string> {
  return new Promise((resolveRun, rejectRun) => {
    execFile(binary, [...args], { encoding: "utf8", maxBuffer: 64 * 1024 }, (error, stdout) => {
      if (error) {
        // Arguments and stderr can carry one-time credentials. Report only the
        // bounded operation name and exit status to failure artifacts.
        rejectRun(new Error(`sparerunner ${args[0] ?? "command"} failed`));
        return;
      }
      resolveRun(stdout);
    });
  });
}

async function waitForController(
  serve: ChildProcessWithoutNullStreams,
): Promise<{ readonly adminURL: string; readonly agentURL: string }> {
  let output = "";
  let exited = false;
  serve.once("exit", () => {
    exited = true;
  });
  serve.stdout.on("data", (chunk: Buffer) => {
    if (output.length < 4096) output += chunk.toString("utf8");
  });
  // Drain stderr without retaining it because future diagnostics may contain
  // security-sensitive material even though production logging forbids that.
  serve.stderr.resume();

  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (exited) throw new Error("Controller exited before publishing its loopback endpoints.");
    const agentURL = output.match(/^Agent endpoint: (https:\/\/127\.0\.0\.1:\d+)$/m)?.[1];
    const adminURL = output.match(/^Web UI: (http:\/\/127\.0\.0\.1:\d+)$/m)?.[1];
    if (agentURL && adminURL) {
      try {
        const response = await fetch(adminURL, { redirect: "error" });
        if (response.ok) return { adminURL, agentURL };
      } catch {
        // The listener can be published just before Serve begins accepting.
      }
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, 50));
  }
  throw new Error("Controller did not become ready on its loopback management endpoint.");
}

async function waitForExit(process: ChildProcessWithoutNullStreams): Promise<void> {
  if (process.exitCode !== null || process.signalCode !== null) return;
  await Promise.race([
    new Promise<void>((resolveExit) => process.once("exit", () => resolveExit())),
    new Promise<void>((resolveTimeout) => setTimeout(resolveTimeout, 2_000)),
  ]);
  if (process.exitCode === null && process.signalCode === null) {
    process.kill("SIGKILL");
    await new Promise<void>((resolveExit) => process.once("exit", () => resolveExit()));
  }
}
