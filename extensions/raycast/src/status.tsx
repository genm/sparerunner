import { Action, ActionPanel, Color, Icon, List, showToast, Toast } from "@raycast/api";
import { useEffect, useState } from "react";
import { callNode, explain, headline, NodeControlError, NodeStatus } from "./node";

export default function Command() {
  const [status, setStatus] = useState<NodeStatus | undefined>();
  const [failure, setFailure] = useState<NodeControlError | undefined>();
  const [loading, setLoading] = useState(true);

  async function load(operation: "status" | "pause" | "resume") {
    setLoading(true);
    try {
      const next = await callNode(operation);
      setStatus(next);
      setFailure(undefined);
      if (operation !== "status") {
        await showToast({ style: Toast.Style.Success, title: headline(next) });
      }
    } catch (error) {
      const controlError =
        error instanceof NodeControlError ? error : new NodeControlError("cli_failed", String(error));
      // An unreachable or refusing agent leaves the previous reading behind
      // rather than repainting it as a fresh healthy state.
      setStatus(undefined);
      setFailure(controlError);
      await showToast({
        style: Toast.Style.Failure,
        title: "Tewake could not read this node",
        message: explain(controlError.errorClass),
      });
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load("status");
  }, []);

  if (failure) {
    return (
      <List isLoading={loading}>
        <List.EmptyView
          icon={{ source: Icon.ExclamationMark, tintColor: Color.Red }}
          title="Unknown — cannot reach the local agent"
          description={`${explain(failure.errorClass)}\n\n${failure.errorClass}: ${failure.message}`}
          actions={
            <ActionPanel>
              <Action title="Retry" icon={Icon.ArrowClockwise} onAction={() => void load("status")} />
            </ActionPanel>
          }
        />
      </List>
    );
  }

  const running = status?.runningExecutions ?? [];
  return (
    <List isLoading={loading}>
      {status && (
        <List.Section title="This computer">
          <List.Item
            icon={stateIcon(status)}
            title={headline(status)}
            accessories={[{ text: status.controllerConnected ? "controller connected" : "controller disconnected" }]}
            actions={
              <ActionPanel>
                {status.intent === "accepting" ? (
                  <Action title="Stop Accepting Jobs" icon={Icon.Pause} onAction={() => void load("pause")} />
                ) : (
                  <Action title="Resume Accepting Jobs" icon={Icon.Play} onAction={() => void load("resume")} />
                )}
                <Action title="Refresh" icon={Icon.ArrowClockwise} onAction={() => void load("status")} />
              </ActionPanel>
            }
          />
          <List.Item
            icon={Icon.Desktop}
            title={`Node ${status.nodeId}`}
            accessories={[{ text: status.intentExplicit ? `set by ${status.intentChangedBy}` : "default" }]}
          />
        </List.Section>
      )}
      <List.Section title={running.length === 0 ? "Running: none" : `Running: ${running.length}`}>
        {running.map((execution) => (
          <List.Item
            key={execution.executionId}
            icon={Icon.Hammer}
            title={execution.executionId}
            accessories={[{ text: execution.state }]}
          />
        ))}
      </List.Section>
    </List>
  );
}

function stateIcon(status: NodeStatus) {
  if (status.intent !== "accepting") {
    return { source: Icon.Pause, tintColor: Color.SecondaryText };
  }
  if (status.pendingResume || !status.nativeRunnerReady) {
    return { source: Icon.Clock, tintColor: Color.Yellow };
  }
  return { source: Icon.CheckCircle, tintColor: Color.Green };
}
