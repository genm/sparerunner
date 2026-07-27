import { showHUD } from "@raycast/api";
import { callNode, explain, headline, NodeControlError } from "./node";

export default async function Command() {
  try {
    const status = await callNode("resume");
    // A resume the controller has not confirmed reports as pending, never as
    // accepting.
    await showHUD(`Tewake: ${headline(status)}`);
  } catch (error) {
    const controlError = error instanceof NodeControlError ? error : new NodeControlError("cli_failed", String(error));
    await showHUD(`Tewake: ${explain(controlError.errorClass)}`);
    throw controlError;
  }
}
