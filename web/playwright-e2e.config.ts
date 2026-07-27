import { defineConfig, devices } from "@playwright/test";

const sensitiveOutputDir = process.env.TEWAKE_E2E_SENSITIVE_OUTPUT_DIR;
if (!sensitiveOutputDir) {
  throw new Error("TEWAKE_E2E_SENSITIVE_OUTPUT_DIR must point to wrapper-owned temporary storage.");
}

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  outputDir: sensitiveOutputDir,
  reporter: [
    ["list"],
    ["junit", { outputFile: "../output/test-results/playwright-e2e-junit.xml" }],
  ],
  use: {
    ...devices["Desktop Chrome"],
    browserName: "chromium",
    // The journey handles one-time credentials. Failure evidence is restricted
    // to JUnit text so traces, screenshots, and video cannot retain them.
    trace: "off",
    screenshot: "off",
    video: "off",
  },
});
