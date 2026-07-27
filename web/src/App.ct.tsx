import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/experimental-ct-react";

import { App } from "./App";
import type { ManagementClient } from "./api/client";
import { createScenarioClient } from "./test/scenario-client";
import type { ScenarioName, ScreenName } from "./ui/state-registry";

type StateCase = {
  readonly scenario: ScenarioName;
  readonly route: ScreenName;
  readonly expectedText: string | RegExp;
};

async function captureState(
  testInfo: { outputPath: (name: string) => string },
  page: { screenshot: (options: { path: string; fullPage: boolean }) => Promise<unknown> },
  name: string,
) {
  await page.screenshot({ path: testInfo.outputPath(`${name}.png`), fullPage: true });
}

const stateCases: readonly StateCase[] = [
  {
    scenario: "loading",
    route: "overview",
    expectedText: /Opening the local management console/i,
  },
  {
    scenario: "empty",
    route: "nodes",
    expectedText: "No enrolled nodes",
  },
  {
    scenario: "running",
    route: "runs",
    expectedText: "running",
  },
  {
    scenario: "offline",
    route: "nodes",
    expectedText: /last known/i,
  },
  {
    scenario: "stale",
    route: "targets",
    expectedText: /last known — stale/i,
  },
  {
    scenario: "permission_error",
    route: "overview",
    expectedText: "Administrator session required",
  },
  {
    scenario: "quarantined",
    route: "nodes",
    expectedText: "Safety quarantine",
  },
  {
    scenario: "stopped_by_owner",
    route: "nodes",
    expectedText: "Stopped by owner",
  },
];

for (const stateCase of stateCases) {
  test(`${stateCase.scenario} remains explicit`, async ({ mount, page }, testInfo) => {
    const component = await mount(
      <App api={createScenarioClient(stateCase.scenario)} initialRoute={stateCase.route} />,
    );
    await expect(component.getByText(stateCase.expectedText)).toBeVisible();
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await captureState(testInfo, page, `${stateCase.scenario}-${testInfo.project.name}`);
  });
}

test("stopped-by-owner node shows adopted exclusions distinctly", async ({
  mount,
  page,
}, testInfo) => {
  const component = await mount(
    <App api={createScenarioClient("stopped_by_owner")} initialRoute="nodes" />,
  );
  await expect(component.getByText("Stopped by owner")).toBeVisible();
  await expect(component.getByText("1 scope excluded")).toBeVisible();
  await expect(component.getByText("1 scope excluded")).toHaveAttribute("title", "target-1");
  await expect(component.getByText("Runner unavailable")).toHaveCount(0);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `stopped-by-owner-${testInfo.project.name}`);
});

test("pending browser handoff remains non-authorizing", async ({ mount, page }, testInfo) => {
  const component = await mount(
    <App api={createScenarioClient("handoff_processing")} initialRoute="setup" />,
  );
  await component.getByRole("button", { name: "Begin browser authorization" }).click();
  await component.getByRole("button", { name: "Continue after authorization" }).click();

  await expect(component.getByText(/Authorization is still pending/i)).toBeVisible();
  await expect(component.getByText(/TWA-TEST-DEVICE-CODE/)).toBeVisible();
  expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);
  expect(page.url()).not.toContain("claimSecret");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `handoff-pending-${testInfo.project.name}`);
});

test("rejected browser handoff is scrubbed", async ({ mount, page }, testInfo) => {
  const component = await mount(
    <App api={createScenarioClient("handoff_rejected")} initialRoute="setup" />,
  );
  await component.getByRole("button", { name: "Begin browser authorization" }).click();
  await component.getByRole("button", { name: "Continue after authorization" }).click();

  await expect(component.getByText(/rejected or expired/i)).toBeVisible();
  await expect(component.getByText(/TWA-TEST-DEVICE-CODE/)).toHaveCount(0);
  expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);
  expect(page.url()).not.toContain("claimSecret");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `handoff-rejected-${testInfo.project.name}`);
});

test("navigation and live-update state remain reachable", async ({ mount, page }, testInfo) => {
  const component = await mount(
    <App api={createScenarioClient("empty")} initialRoute="overview" />,
  );

  await expect(component.getByText("Live updates reconnecting")).toBeVisible();
  for (const label of ["Setup", "Overview", "Nodes", "Targets", "Runs", "Settings"]) {
    await expect(component.getByRole("link", { name: label, exact: true })).toBeVisible();
  }
  const settingsBox = await component
    .getByRole("link", { name: "Settings", exact: true })
    .boundingBox();
  const viewport = page.viewportSize();
  expect(settingsBox).not.toBeNull();
  expect(viewport).not.toBeNull();
  expect(settingsBox!.x).toBeGreaterThanOrEqual(0);
  expect(settingsBox!.x + settingsBox!.width).toBeLessThanOrEqual(viewport!.width);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `navigation-${testInfo.project.name}`);
});

test("field validation identifies the invalid control", async ({ mount, page }, testInfo) => {
  const component = await mount(
    <App api={createScenarioClient("validation_error")} initialRoute="settings" />,
  );
  const maximum = component.getByRole("spinbutton", { name: "Maximum runners" });
  await component.getByRole("button", { name: "Apply configuration" }).click();

  await expect(maximum).toHaveAttribute("aria-invalid", "true");
  await expect(maximum).toHaveAttribute("aria-describedby", "scheduler-max-error");
  await expect(component.locator("#scheduler-max-error")).toHaveAttribute("role", "alert");
  await expect(component.locator("#scheduler-max-error")).toHaveText("must be positive");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `validation-error-${testInfo.project.name}`);
});

test("connected GitHub Target dialog exposes only the verified creation journey", async ({
  mount,
  page,
}, testInfo) => {
  const base = createScenarioClient("empty");
  const api: ManagementClient = {
    ...base,
    getSetup: async () => ({
      ...(await base.getSetup()),
      githubAppState: "connected",
      manifestFlowSupported: true,
    }),
    listGitHubInstallations: async () => ({
      installations: [
        { id: "42", accountLogin: "acme", accountType: "Organization", repositorySelection: "all" },
      ],
    }),
    createGitHubTarget: async () => ({
      target: {
        id: "target-test",
        installationId: "42",
        scopeKind: "repository",
        scope: "acme/private",
        scaleSetName: "tewake",
        runnerProfileId: "profile-tewake",
        status: "ready",
        freshness: { state: "unknown" },
      },
      configurationRevision: "1",
    }),
  };
  const component = await mount(<App api={api} initialRoute="targets" />);
  await component.getByRole("button", { name: "Create target" }).click();
  await expect(component.getByRole("dialog")).toBeVisible();
  await expect(component.getByRole("combobox", { name: "GitHub account" })).toHaveValue("42");
  await expect(component.getByRole("button", { name: "Verify and create" })).toBeVisible();
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await captureState(testInfo, page, `target-dialog-${testInfo.project.name}`);
});
