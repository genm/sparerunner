import { describe, expect, it } from "vitest";

import { digestBrowserClaimSecret } from "./browser-handoff";

describe("browser handoff claim encoding", () => {
  it("matches the Go SHA-256 raw-base64url vector", async () => {
    const rawSecret = Uint8Array.from({ length: 32 }, (_, index) => index);

    await expect(digestBrowserClaimSecret(rawSecret)).resolves.toBe(
      "Yw3NKWbEM2aRElRIu7JbT_QSpJxzLbLIq8G4WBvXEN0",
    );
  });
});
