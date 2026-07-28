import { Action, ActionPanel, Color, Icon, List, showToast, Toast } from "@raycast/api";
import { useEffect, useState } from "react";
import {
  callNode,
  callNodeTarget,
  explain,
  headline,
  NodeControlError,
  NodeStatus,
  targetStateLabel,
} from "./node";

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
        title: "SpareRunner could not read this node",
        message: explain(controlError.errorClass),
      });
    } finally {
      setLoading(false);
    }
  }

  async function loadTarget(action: "exclude" | "include", targetId: string) {
    setLoading(true);
    try {
      const next = await callNodeTarget(action, targetId);
      setStatus(next);
      setFailure(undefined);
      await showToast({
        style: Toast.Style.Success,
        title: action === "exclude" ? `Excluded ${targetId}` : `Included ${targetId}`,
      });
    } catch (error) {
      const controlError =
        error instanceof NodeControlError ? error : new NodeControlError("cli_failed", String(error));
      setFailure(controlError);
      await showToast({
        style: Toast.Style.Failure,
        title: "SpareRunner could not update this Target",
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
      {status && (
        <List.Section
          title={
            status.eligibleTargets === undefined
              ? "Targets: not yet reported"
              : `Targets: ${status.eligibleTargets.length}`
          }
        >
          {(status.eligibleTargets ?? []).map((target) => (
            <List.Item
              key={target.targetId}
              icon={targetIcon(target.locallyExcluded)}
              title={target.scope}
              subtitle={target.scopeKind}
              accessories={[{ text: targetStateLabel(target) }]}
              actions={
                <ActionPanel>
                  {target.locallyExcluded ? (
                    <Action
                      title="Include Target"
                      icon={Icon.Plus}
                      onAction={() => void loadTarget("include", target.targetId)}
                    />
                  ) : (
                    <Action
                      title="Exclude Target"
                      icon={Icon.Minus}
                      onAction={() => void loadTarget("exclude", target.targetId)}
                    />
                  )}
                  <Action title="Refresh" icon={Icon.ArrowClockwise} onAction={() => void load("status")} />
                </ActionPanel>
              }
            />
          ))}
        </List.Section>
      )}
      {status && (status.unknownExclusions?.length ?? 0) > 0 && (
        <List.Section title="Excluded, not currently eligible">
          {status.unknownExclusions!.map((targetId) => (
            <List.Item
              key={targetId}
              icon={{ source: Icon.QuestionMark, tintColor: Color.SecondaryText }}
              title={targetId}
              accessories={[{ text: "not currently eligible" }]}
              actions={
                <ActionPanel>
                  <Action
                    title="Include Target"
                    icon={Icon.Plus}
                    onAction={() => void loadTarget("include", targetId)}
                  />
                </ActionPanel>
              }
            />
          ))}
        </List.Section>
      )}
    </List>
  );
}

function targetIcon(locallyExcluded: boolean) {
  return locallyExcluded
    ? { source: Icon.MinusCircle, tintColor: Color.SecondaryText }
    : { source: Icon.CheckCircle, tintColor: Color.Green };
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
