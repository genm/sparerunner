import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { App } from "./App";

afterEach(cleanup);

describe("App", () => {
  it("states the trusted-computer product boundary", () => {
    render(<App />);

    expect(screen.getByRole("heading", { level: 1, name: "Tewake" })).toBeTruthy();
    expect(screen.getByText(/trusted computers/i)).toBeTruthy();
  });
});
