import { showHUD } from "@raycast/api";
import { callNode, explain, headline, NodeControlError } from "./node";

export default async function Command() {
  try {
    const status = await callNode("pause");
    await showHUD(`SpareRunner: ${headline(status)}`);
  } catch (error) {
    const controlError = error instanceof NodeControlError ? error : new NodeControlError("cli_failed", String(error));
    // Failing loudly keeps the launcher from implying a state change that the
    // agent never recorded.
    await showHUD(`SpareRunner: ${explain(controlError.errorClass)}`);
    throw controlError;
  }
}
