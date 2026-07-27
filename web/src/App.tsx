import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  ApiError,
  createManagementClient,
  type ManagementClient,
  type Problem,
  type Schema,
} from "./api/client";
import { createBrowserClaimSecret, digestBrowserClaimSecret } from "./security/browser-handoff";
import { resourceForScreen, type ScreenName, type Snapshot } from "./ui/state-registry";

const navigation: readonly { readonly id: ScreenName; readonly label: string }[] = [
  { id: "setup", label: "Setup" },
  { id: "overview", label: "Overview" },
  { id: "nodes", label: "Nodes" },
  { id: "targets", label: "Targets" },
  { id: "runs", label: "Runs" },
  { id: "settings", label: "Settings" },
];

type AppProps = { readonly api?: ManagementClient; readonly initialRoute?: ScreenName };
type LoadState = "booting" | "ready" | "handoff-required" | "unavailable";
type Toast = { readonly tone: "success" | "error"; readonly message: string } | undefined;

// Production renders must retain one client identity; recreating it on every
// render would restart session loading and SSE effects after each state update.
const defaultManagementClient = createManagementClient();

export function App({ api: suppliedAPI, initialRoute }: AppProps) {
  const api = suppliedAPI ?? defaultManagementClient;
  const [route, setRoute] = useState<ScreenName>(() => initialRoute ?? hashRoute());
  const [loadState, setLoadState] = useState<LoadState>("booting");
  const [session, setSession] = useState<Schema["Session"]>();
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [problem, setProblem] = useState<Problem>();
  const [readDegraded, setReadDegraded] = useState(false);
  const [liveUpdates, setLiveUpdates] = useState<"connected" | "reconnecting">("reconnecting");
  const [auditRevision, setAuditRevision] = useState(0);
  const [toast, setToast] = useState<Toast>();
  const headingRef = useRef<HTMLHeadingElement>(null);
  const snapshotRef = useRef<Snapshot | undefined>(undefined);

  useEffect(() => {
    snapshotRef.current = snapshot;
  }, [snapshot]);

  const refresh = useCallback(async (): Promise<boolean> => {
    try {
      const [setup, overview, nodeResult, targetResult, runResult, configuration] =
        await Promise.all([
          api.getSetup(),
          api.getOverview(),
          api.listNodes(),
          api.listTargets(),
          api.listRuns(),
          api.getConfiguration(),
        ]);
      setSnapshot({
        setup,
        overview,
        nodes: nodeResult.nodes,
        nodesConfigurationRevision: nodeResult.configurationRevision,
        targets: targetResult.targets,
        targetsConfigurationRevision: targetResult.configurationRevision,
        runs: runResult.runs,
        configuration,
      });
      setProblem(undefined);
      setReadDegraded(false);
      return true;
    } catch (error) {
      const apiError = apiErrorShape(error);
      // A failed refresh must not erase a previously confirmed fleet view. In particular,
      // GitHub/controller 503s are shown as degraded last-known state, never as an empty fleet.
      if (snapshotRef.current && apiError?.status === 503) {
        setProblem(apiError.problem);
        setReadDegraded(true);
        setLoadState("ready");
        return false;
      }
      handleReadError(error, setLoadState, setProblem);
      return false;
    }
  }, [api]);

  useEffect(() => {
    let active = true;
    void (async () => {
      try {
        const currentSession = await api.getSession();
        if (!active) return;
        setSession(currentSession);
        if (await refresh()) {
          if (active) setLoadState("ready");
        }
      } catch (error) {
        if (active) handleReadError(error, setLoadState, setProblem);
      }
    })();
    return () => {
      active = false;
    };
  }, [api, refresh]);

  useEffect(() => {
    const onHashChange = () => setRoute(hashRoute());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    if (loadState === "ready") headingRef.current?.focus();
  }, [route, loadState]);

  useEffect(() => {
    if (!session) return undefined;
    return api.subscribe(session.csrfToken, undefined, {
      onEvent: (event) => {
        setLiveUpdates("connected");
        if (event.kind !== "ready" && event.resources.includes("audit_events")) {
          setAuditRevision((current) => current + 1);
        }
        if (
          event.kind !== "ready" &&
          event.resources.some((resource) => resourceForScreen[route].includes(resource))
        ) {
          void refresh();
        }
      },
      onError: (failure) => {
        if (failure.kind === "authentication") {
          snapshotRef.current = undefined;
          setSnapshot(undefined);
          setSession(undefined);
          setProblem(undefined);
          setReadDegraded(false);
          setToast(undefined);
          setLoadState("handoff-required");
          setLiveUpdates("reconnecting");
          return;
        }
        setLiveUpdates("reconnecting");
      },
    });
  }, [api, refresh, route, session]);

  const navigate = (next: ScreenName) => {
    if (window.location.hash !== `#/${next}`) window.location.hash = `/${next}`;
    setRoute(next);
  };

  const authenticated = loadState === "ready" && session && snapshot;
  if (!authenticated) {
    return (
      <UnauthenticatedSurface
        api={api}
        state={loadState}
        problem={problem}
        onAuthenticated={(next) => {
          setSession(next);
          setLoadState("booting");
          setProblem(undefined);
          void refresh().then((refreshed) => {
            if (refreshed) setLoadState("ready");
          });
        }}
      />
    );
  }

  return (
    <div className="app-shell">
      <a className="skip-link" href="#page-content">
        Skip to page content
      </a>
      <aside className="side-rail" aria-label="Tewake navigation">
        <Brand />
        <nav className="route-nav" aria-label="Management sections">
          {navigation.map((item) => (
            <a
              aria-current={route === item.id ? "page" : undefined}
              href={`#/${item.id}`}
              key={item.id}
              onClick={() => navigate(item.id)}
            >
              {item.label}
            </a>
          ))}
        </nav>
        <p className="rail-note">Loopback management · single owner</p>
      </aside>
      <div className="console">
        <header className="console-header">
          <span className="controller-label">Controller</span>
          <span aria-live="polite" className="live-status">
            {liveUpdates === "connected" ? "Live updates connected" : "Live updates reconnecting"}
          </span>
        </header>
        <main id="page-content" className="page-content" aria-busy={!snapshot}>
          <Page
            api={api}
            csrfToken={session.csrfToken}
            headingRef={headingRef}
            onRefresh={refresh}
            onToast={setToast}
            route={route}
            snapshot={snapshot}
            mutationsDisabled={readDegraded}
            auditRevision={auditRevision}
          />
          {readDegraded ? (
            <p className="degraded-banner" role="status">
              Live management data is unavailable. Showing the last confirmed fleet state; actions
              are disabled until a refresh succeeds.
            </p>
          ) : null}
        </main>
      </div>
      {toast ? (
        <div
          className={`toast toast-${toast.tone}`}
          role={toast.tone === "error" ? "alert" : "status"}
        >
          {toast.message}
        </div>
      ) : null}
    </div>
  );
}

function UnauthenticatedSurface({
  api,
  state,
  problem,
  onAuthenticated,
}: {
  readonly api: ManagementClient;
  readonly state: LoadState;
  readonly problem?: Problem;
  readonly onAuthenticated: (session: Schema["Session"]) => void;
}) {
  if (state === "booting") {
    return (
      <main className="auth-surface" aria-busy="true">
        <Brand />
        <h1>Opening local management console</h1>
        <p className="loading-copy">Opening the local management console…</p>
      </main>
    );
  }
  if (state === "unavailable") {
    return (
      <main className="auth-surface">
        <Brand />
        <h1>Management authority unavailable</h1>
        <p>
          The controller did not confirm a safe management response. Reload after the local
          authority recovers.
        </p>
        <ProblemReference problem={problem} />
        <button onClick={() => window.location.reload()} type="button">
          Reload console
        </button>
      </main>
    );
  }
  return <BrowserHandoff api={api} onAuthenticated={onAuthenticated} />;
}

function BrowserHandoff({
  api,
  onAuthenticated,
}: {
  readonly api: ManagementClient;
  readonly onAuthenticated: (session: Schema["Session"]) => void;
}) {
  const [delivery, setDelivery] = useState<{
    readonly code: string;
    readonly secret: string;
    readonly expiresAt: string;
  }>();
  const [message, setMessage] = useState<string>();
  const [busy, setBusy] = useState(false);

  const begin = async () => {
    setBusy(true);
    setMessage(undefined);
    try {
      const secret = createBrowserClaimSecret();
      const created = await api.createBrowserHandoff(await digestBrowserClaimSecret(secret.bytes));
      setDelivery({ code: created.code, secret: secret.value, expiresAt: created.expiresAt });
    } catch (error) {
      setMessage(errorMessage(error));
    } finally {
      setBusy(false);
    }
  };
  const continueHandoff = async () => {
    if (!delivery) return;
    setBusy(true);
    setMessage(undefined);
    const current = delivery;
    // The claim secret is intentionally removed from React state before transport.
    setDelivery((previous) => (previous ? { ...previous, secret: "" } : previous));
    try {
      const result = await api.claimBrowserHandoff(current.code, current.secret);
      if (result.status === "authenticated") {
        setDelivery(undefined);
        onAuthenticated(result.session);
        return;
      }
      setMessage(
        "Authorization is still pending. Complete the command on the Controller host, then continue.",
      );
      setDelivery(current);
    } catch (error) {
      const claimError = apiErrorShape(error);
      if (claimError?.status === 401 || claimError?.status === 410) {
        setDelivery(undefined);
        setMessage(
          "The authorization code was rejected or expired. Start a fresh browser authorization.",
        );
        return;
      }

      // The response body can be interrupted after Set-Cookie was accepted. Read the
      // current authority before calling that committed outcome a rejection.
      try {
        const currentSession = await api.getSession();
        setDelivery(undefined);
        onAuthenticated(currentSession);
      } catch {
        setDelivery(current);
        setMessage(
          "The authorization result could not be confirmed. Keep this page open and retry after the Controller recovers, or start over.",
        );
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="auth-surface">
      <Brand />
      <h1>Administrator session required</h1>
      <p>
        This loopback console never creates an administrator session by itself. Authorize this
        browser from the Controller host.
      </p>
      {delivery ? (
        <section aria-labelledby="handoff-code-title" className="handoff-card">
          <h2 id="handoff-code-title">Authorize this browser</h2>
          <p>
            Run this command on the Controller host. The displayed code is not a credential and
            expires at {formatDate(delivery.expiresAt)}.
          </p>
          <code className="device-command">tewake ui authorize '{delivery.code}'</code>
          <button disabled={busy} onClick={() => void continueHandoff()} type="button">
            {busy ? "Checking authorization…" : "Continue after authorization"}
          </button>
          <button
            className="button-quiet"
            disabled={busy}
            onClick={() => {
              setDelivery(undefined);
              setMessage(undefined);
            }}
            type="button"
          >
            Start over
          </button>
        </section>
      ) : (
        <button disabled={busy} onClick={() => void begin()} type="button">
          {busy ? "Preparing authorization…" : "Begin browser authorization"}
        </button>
      )}
      {message ? (
        <p className="inline-problem" role="alert">
          {message}
        </p>
      ) : null}
    </main>
  );
}

function Page({
  api,
  csrfToken,
  headingRef,
  onRefresh,
  onToast,
  route,
  snapshot,
  mutationsDisabled,
  auditRevision,
}: {
  readonly api: ManagementClient;
  readonly csrfToken: string;
  readonly headingRef: React.RefObject<HTMLHeadingElement | null>;
  readonly onRefresh: () => Promise<boolean>;
  readonly onToast: (value: Toast) => void;
  readonly route: ScreenName;
  readonly snapshot: Snapshot;
  readonly mutationsDisabled: boolean;
  readonly auditRevision: number;
}) {
  const heading = navigation.find((item) => item.id === route)?.label ?? "Overview";
  const content = useMemo(() => {
    switch (route) {
      case "setup":
        return (
          <SetupPage
            api={api}
            csrfToken={csrfToken}
            snapshot={snapshot}
            onRefresh={onRefresh}
            onToast={onToast}
            mutationsDisabled={mutationsDisabled}
          />
        );
      case "overview":
        return <OverviewPage snapshot={snapshot} />;
      case "nodes":
        return (
          <NodesPage
            api={api}
            csrfToken={csrfToken}
            snapshot={snapshot}
            onRefresh={onRefresh}
            onToast={onToast}
            mutationsDisabled={mutationsDisabled}
          />
        );
      case "targets":
        return <TargetsPage snapshot={snapshot} />;
      case "runs":
        return <RunsPage snapshot={snapshot} />;
      case "settings":
        return (
          <SettingsPage
            api={api}
            csrfToken={csrfToken}
            snapshot={snapshot}
            onRefresh={onRefresh}
            onToast={onToast}
            mutationsDisabled={mutationsDisabled}
            auditRevision={auditRevision}
          />
        );
    }
  }, [api, auditRevision, csrfToken, mutationsDisabled, onRefresh, onToast, route, snapshot]);
  return (
    <>
      <header className="page-header">
        <p className="eyebrow">Tewake management</p>
        <h1 ref={headingRef} tabIndex={-1}>
          {heading}
        </h1>
      </header>
      {content}
    </>
  );
}

function SetupPage({ api, csrfToken, snapshot, onRefresh, onToast, mutationsDisabled }: PageProps) {
  const [open, setOpen] = useState(false);
  const joinCodeButtonRef = useRef<HTMLButtonElement>(null);
  const closeJoinCode = useCallback(() => setOpen(false), []);
  return (
    <>
      <div className="setup-grid">
        <Step complete={snapshot.setup.controllerInitialized} index="01" title="Controller">
          Local controller identity is ready.
        </Step>
        <Step
          complete={snapshot.setup.githubAppState === "connected"}
          index="02"
          title="GitHub App"
        >
          <p>
            {snapshot.setup.githubAppState === "degraded"
              ? "Last-known GitHub authority is degraded."
              : "Connect a user-owned GitHub App before creating a Target."}
          </p>
          <DisabledAction
            reason={
              snapshot.setup.manifestFlowSupported
                ? undefined
                : "GitHub App handoff is not available in this controller build."
            }
          >
            Connect GitHub
          </DisabledAction>
        </Step>
        <Step complete={snapshot.setup.nodeCount > 0} index="03" title="Nodes">
          <p>{snapshot.setup.nodeCount} enrolled</p>
          <button
            disabled={mutationsDisabled}
            onClick={() => setOpen(true)}
            ref={joinCodeButtonRef}
            type="button"
          >
            Create join code
          </button>
        </Step>
        <Step complete={snapshot.setup.targetCount > 0} index="04" title="Targets">
          <p>{snapshot.setup.targetCount} configured</p>
          <DisabledAction reason="A verified GitHub installation is required before a Target can be created.">
            Create target
          </DisabledAction>
        </Step>
      </div>
      <ConditionStrip conditions={snapshot.setup.conditions} />
      {open ? (
        <JoinCodeDialog
          api={api}
          csrfToken={csrfToken}
          onClose={closeJoinCode}
          onRefresh={onRefresh}
          onToast={onToast}
          returnFocusRef={joinCodeButtonRef}
        />
      ) : null}
    </>
  );
}

function OverviewPage({ snapshot }: { readonly snapshot: Snapshot }) {
  return (
    <>
      <ConditionStrip conditions={snapshot.overview.conditions} />
      <section aria-label="Fleet overview" className="metric-grid">
        <Metric label="Configured slots" value={String(snapshot.overview.configuredCapacity)} />
        <Metric label="Active runs" value={String(snapshot.overview.activeRuns)} />
        <Metric label="Nodes" value={String(snapshot.overview.nodeCount)} />
        <Metric label="Targets" value={String(snapshot.overview.targetCount)} />
      </section>
      <section className="section-block" aria-labelledby="active-runs-heading">
        <div className="section-heading">
          <h2 id="active-runs-heading">Active runs</h2>
          <span>{snapshot.runs.length} observed</span>
        </div>
        {snapshot.runs.length ? (
          <RunRows runs={snapshot.runs} />
        ) : (
          <EmptyPanel
            title="No active runs"
            detail="Jobs appear here after GitHub assigns a scale-set message to this fleet."
          />
        )}
      </section>
    </>
  );
}

function NodesPage({ api, csrfToken, snapshot, onRefresh, onToast, mutationsDisabled }: PageProps) {
  const [pending, setPending] = useState<string>();
  const mutate = async (node: Schema["Node"], action: "drain" | "resume" | "revoke") => {
    setPending(`${node.id}:${action}`);
    try {
      await api.setNodeState(node.id, action, snapshot.nodesConfigurationRevision, csrfToken);
      onToast({
        tone: "success",
        message:
          action === "drain"
            ? `${node.displayName} is draining.`
            : `${node.displayName} state updated.`,
      });
      await onRefresh();
    } catch (error) {
      if (isCommittedMutation(error)) {
        const refreshed = await onRefresh();
        onToast({
          tone: "error",
          message: refreshed
            ? "The node update committed but its response was not confirmed. Current state was reloaded."
            : "The node update may have committed. Actions remain disabled until current state can be reloaded.",
        });
      } else {
        onToast({ tone: "error", message: errorMessage(error) });
      }
    } finally {
      setPending(undefined);
    }
  };
  return (
    <section className="section-block" aria-labelledby="nodes-list-heading">
      <div className="section-heading">
        <h2 id="nodes-list-heading">Fleet nodes</h2>
        <span>{snapshot.nodes.length} enrolled</span>
      </div>
      {snapshot.nodes.length ? (
        <div className="node-list">
          {snapshot.nodes.map((node) => {
            const action =
              node.administrativeState === "active"
                ? "drain"
                : node.administrativeState === "draining"
                  ? "resume"
                  : undefined;
            return (
              <article className="node-row" key={node.id}>
                <NodeStatus node={node} />
                <div className="node-identity">
                  <strong>{node.displayName}</strong>
                  <span>{node.id}</span>
                </div>
                <div className="node-platform">
                  <span>
                    {node.operatingSystem ?? "Unknown"} · {node.architecture ?? "Unknown"}
                  </span>
                  <span>{node.runnerVersion ?? "Runner version not reported"}</span>
                </div>
                <div className="node-capacity">
                  <strong>
                    {node.activeRunnerCount} / {node.maxRunners}
                  </strong>
                  <span>
                    {node.availableMemoryBytes
                      ? `${formatBytes(node.availableMemoryBytes)} available`
                      : "Memory not reported"}
                  </span>
                </div>
                <div className="node-action">
                  {action ? (
                    <button
                      disabled={mutationsDisabled || pending === `${node.id}:${action}`}
                      onClick={() => void mutate(node, action)}
                      type="button"
                    >
                      {pending === `${node.id}:${action}`
                        ? `${capitalize(action)}ing…`
                        : capitalize(action)}
                    </button>
                  ) : (
                    <NodeTerminalState node={node} />
                  )}
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <EmptyPanel
          title="No enrolled nodes"
          detail="Create a join code, then run tewake join on a trusted computer."
        />
      )}
    </section>
  );
}

function TargetsPage({ snapshot }: { readonly snapshot: Snapshot }) {
  return (
    <section className="section-block" aria-labelledby="targets-heading">
      <div className="section-heading">
        <h2 id="targets-heading">GitHub Targets</h2>
        <DisabledAction reason="A verified GitHub installation is required before a Target can be created.">
          Create target
        </DisabledAction>
      </div>
      {snapshot.targets.length ? (
        <div className="target-list">
          {snapshot.targets.map((target) => (
            <article className="target-row" key={target.id}>
              <div>
                <strong>{target.scope}</strong>
                <span>
                  {target.scopeKind} · {target.scaleSetName}
                </span>
              </div>
              <div>
                <strong>{target.status}</strong>
                <Freshness freshness={target.freshness} />
              </div>
            </article>
          ))}
        </div>
      ) : (
        <EmptyPanel
          title="No GitHub Targets"
          detail="Connect a verified GitHub App to create a private repository or organization Target."
        />
      )}
    </section>
  );
}

function RunsPage({ snapshot }: { readonly snapshot: Snapshot }) {
  return (
    <section className="section-block" aria-labelledby="runs-heading">
      <div className="section-heading">
        <h2 id="runs-heading">Execution history</h2>
        <span>{snapshot.runs.length} observed</span>
      </div>
      {snapshot.runs.length ? (
        <RunRows runs={snapshot.runs} />
      ) : (
        <EmptyPanel
          title="No executions"
          detail="This is a normal empty state until GitHub assigns work to a configured Target."
        />
      )}
    </section>
  );
}

function SettingsPage({
  api,
  auditRevision,
  csrfToken,
  snapshot,
  onRefresh,
  onToast,
  mutationsDisabled,
}: PageProps & { readonly auditRevision: number }) {
  const [draft, setDraft] = useState(snapshot.configuration);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [reloadRequired, setReloadRequired] = useState(false);
  const [remoteChanged, setRemoteChanged] = useState(false);
  const [problem, setProblem] = useState<Problem>();
  const [yaml, setYAML] = useState<string>();
  const schedulerMaxError = fieldProblem(problem, "scheduler.maxRunners");
  useEffect(() => {
    if (!dirty && !reloadRequired) {
      setDraft(snapshot.configuration);
      setRemoteChanged(false);
      return;
    }
    if (snapshot.configuration.revision !== draft.revision) {
      // Live invalidations must not silently discard an operator's in-progress draft.
      setRemoteChanged(true);
    }
  }, [dirty, draft.revision, reloadRequired, snapshot.configuration]);
  const apply = async () => {
    setSaving(true);
    setProblem(undefined);
    try {
      const applied = await api.applyConfiguration(draft, csrfToken);
      setDraft(applied);
      setDirty(false);
      setRemoteChanged(false);
      setReloadRequired(false);
      onToast({ tone: "success", message: "Configuration applied." });
      await onRefresh();
    } catch (error) {
      const apiError = apiErrorShape(error);
      if (apiError?.problem) setProblem(apiError.problem);
      if (apiError?.status === 409) {
        setRemoteChanged(true);
        await onRefresh();
      } else if (apiError?.problem?.code === "mutation_committed_reload_required") {
        // The server may have committed while the response was interrupted. Never retry against
        // an uncertain revision: keep save disabled until the operator reviews a fresh read.
        setReloadRequired(true);
        setRemoteChanged(true);
        await onRefresh();
      }
      onToast({ tone: "error", message: errorMessage(error) });
    } finally {
      setSaving(false);
    }
  };
  const loadConfirmed = () => {
    setDraft(snapshot.configuration);
    setDirty(false);
    setReloadRequired(false);
    setRemoteChanged(false);
    setProblem(undefined);
  };
  const exportYAML = async () => {
    try {
      setYAML(await api.exportConfiguration());
    } catch (error) {
      onToast({ tone: "error", message: errorMessage(error) });
    }
  };
  return (
    <div className="settings-stack">
      <section className="section-block" aria-labelledby="capacity-heading">
        <div className="section-heading">
          <h2 id="capacity-heading">Fleet capacity</h2>
          <span>Revision {draft.revision}</span>
        </div>
        <label className="field">
          <span>Maximum runners</span>
          <input
            aria-describedby={schedulerMaxError ? "scheduler-max-error" : undefined}
            aria-invalid={schedulerMaxError ? true : undefined}
            min="1"
            onChange={(event) => {
              setDirty(true);
              setDraft({ ...draft, scheduler: { maxRunners: Number(event.target.value) } });
            }}
            type="number"
            value={draft.scheduler.maxRunners ?? ""}
          />
          {schedulerMaxError ? (
            <small id="scheduler-max-error" className="field-error" role="alert">
              {schedulerMaxError}
            </small>
          ) : null}
        </label>
        <button
          disabled={saving || mutationsDisabled || reloadRequired || remoteChanged}
          onClick={() => void apply()}
          type="button"
        >
          {saving ? "Saving…" : "Apply configuration"}
        </button>
        {remoteChanged && !reloadRequired ? (
          <p className="inline-problem" role="alert">
            Configuration changed elsewhere. Load the confirmed revision and review it before
            applying another change.
          </p>
        ) : null}
        {reloadRequired ? (
          <p className="inline-problem" role="alert">
            The save result is uncertain. Refreshing the authoritative configuration before another
            change.
          </p>
        ) : null}
        {remoteChanged || reloadRequired ? (
          <button className="button-quiet" onClick={loadConfirmed} type="button">
            Load confirmed configuration
          </button>
        ) : null}
      </section>
      <section className="section-block" aria-labelledby="export-heading">
        <div className="section-heading">
          <h2 id="export-heading">Configuration export</h2>
          <span>Non-secret YAML</span>
        </div>
        <button onClick={() => void exportYAML()} type="button">
          Export YAML
        </button>
        {yaml ? <textarea aria-label="Exported configuration YAML" readOnly value={yaml} /> : null}
      </section>
      <AuditEvents api={api} invalidation={auditRevision} />
    </div>
  );
}

function AuditEvents({
  api,
  invalidation,
}: {
  readonly api: ManagementClient;
  readonly invalidation: number;
}) {
  const [events, setEvents] = useState<Schema["AuditEvent"][]>([]);
  const [cursor, setCursor] = useState<string>();
  const [resumeCursor, setResumeCursor] = useState<string>();
  const [loaded, setLoaded] = useState(false);
  const [behind, setBehind] = useState(false);
  const [error, setError] = useState<string>();
  const handledInvalidation = useRef(invalidation);
  const loading = useRef(false);
  const refreshPending = useRef(false);
  const load = useCallback(
    async (startCursor: string | undefined, append: boolean) => {
      if (loading.current) {
        refreshPending.current = true;
        setBehind(true);
        return;
      }
      loading.current = true;
      setError(undefined);
      try {
        const page = await api.listAuditEvents(startCursor);
        setEvents((previous) => {
          if (!append) return page.events;
          const known = new Set(previous.map((event) => event.id));
          return [...previous, ...page.events.filter((event) => !known.has(event.id))];
        });
        setCursor(page.nextCursor);
        setResumeCursor(page.resumeCursor);
        setLoaded(true);
        setBehind(Boolean(page.nextCursor) || refreshPending.current);
      } catch (reason) {
        setError(errorMessage(reason));
        setBehind(true);
      } finally {
        refreshPending.current = false;
        loading.current = false;
      }
    },
    [api],
  );

  useEffect(() => {
    if (!loaded || invalidation === handledInvalidation.current) return;
    handledInvalidation.current = invalidation;
    if (cursor) {
      // The operator has not reached the previous tail yet. Preserve pagination
      // order and make the newer append explicit instead of skipping history.
      setBehind(true);
      return;
    }
    void load(resumeCursor, true);
  }, [cursor, invalidation, load, loaded, resumeCursor]);

  const loadNext = async () => {
    try {
      await load(cursor, loaded);
    } finally {
      handledInvalidation.current = invalidation;
    }
  };
  return (
    <section className="section-block" aria-labelledby="audit-heading">
      <div className="section-heading">
        <h2 id="audit-heading">Activity</h2>
        <span>Cursor-paginated</span>
      </div>
      {!loaded ? (
        <button onClick={() => void loadNext()} type="button">
          Load activity
        </button>
      ) : events.length ? (
        <ol className="audit-list">
          {events.map((event) => (
            <li key={event.id}>
              <span>{formatDate(event.occurredAt)}</span>
              <strong>{event.action}</strong>
              <span>{event.outcome}</span>
            </li>
          ))}
        </ol>
      ) : (
        <EmptyPanel title="No recorded activity" detail="New safe audit events will appear here." />
      )}
      {cursor ? (
        <button onClick={() => void loadNext()} type="button">
          Load more
        </button>
      ) : null}
      {behind ? (
        <p className="inline-problem" role="status">
          Newer activity is available.{" "}
          {cursor
            ? "Load remaining pages in order to catch up."
            : "Check again after the Controller recovers."}
        </p>
      ) : null}
      {behind && !cursor ? (
        <button onClick={() => void load(resumeCursor, true)} type="button">
          Check for newer activity
        </button>
      ) : null}
      {error ? (
        <p className="inline-problem" role="alert">
          {error}
        </p>
      ) : null}
    </section>
  );
}

function JoinCodeDialog({
  api,
  csrfToken,
  onClose,
  onRefresh,
  onToast,
  returnFocusRef,
}: {
  readonly api: ManagementClient;
  readonly csrfToken: string;
  readonly onClose: () => void;
  readonly onRefresh: () => Promise<boolean>;
  readonly onToast: (value: Toast) => void;
  readonly returnFocusRef: React.RefObject<HTMLButtonElement | null>;
}) {
  const [hints, setHints] = useState("");
  const [delivery, setDelivery] = useState<Schema["JoinCodeDelivery"]>();
  const [busy, setBusy] = useState(false);
  const dialogRef = useRef<HTMLDialogElement>(null);
  const deliveryRef = useRef<Schema["JoinCodeDelivery"] | undefined>(undefined);
  const mountedRef = useRef(false);
  const createEpochRef = useRef(0);

  const cancelRemote = useCallback(
    (candidate: Schema["JoinCodeDelivery"]) => {
      void api
        .cancelJoinCode(candidate.tokenId, csrfToken)
        .then(() => onRefresh())
        .catch((error) => {
          if (apiErrorShape(error)?.status === 404) {
            // A consumed token is already absent, which satisfies the dialog's
            // teardown postcondition without pretending another cancel committed.
            void onRefresh();
            return;
          }
          onToast({ tone: "error", message: "The join code could not be cancelled." });
        });
    },
    [api, csrfToken, onRefresh, onToast],
  );

  const scrubAndCancel = useCallback(() => {
    // Invalidate an in-flight create before scrubbing the currently revealed credential.
    createEpochRef.current += 1;
    const candidate = deliveryRef.current;
    deliveryRef.current = undefined;
    setDelivery(undefined);
    if (candidate) cancelRemote(candidate);
  }, [cancelRemote]);

  useEffect(() => {
    mountedRef.current = true;
    const dialog = dialogRef.current;
    if (dialog && !dialog.open) dialog.showModal();
    const handleCancel = (event: Event) => {
      // Native Escape must use the same scrub/cancel path as the visible close action.
      event.preventDefault();
      scrubAndCancel();
      onClose();
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        scrubAndCancel();
        onClose();
      }
    };
    dialog?.addEventListener("cancel", handleCancel);
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      mountedRef.current = false;
      createEpochRef.current += 1;
      dialog?.removeEventListener("cancel", handleCancel);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      const candidate = deliveryRef.current;
      deliveryRef.current = undefined;
      if (candidate) cancelRemote(candidate);
      if (dialog?.open) dialog.close();
      returnFocusRef.current?.focus();
    };
  }, [cancelRemote, onClose, returnFocusRef, scrubAndCancel]);

  const close = () => {
    scrubAndCancel();
    onClose();
  };
  const create = async (event: FormEvent) => {
    event.preventDefault();
    const createEpoch = createEpochRef.current;
    setBusy(true);
    try {
      const result = await api.createJoinCode(
        hints
          .split("\n")
          .map((hint) => hint.trim())
          .filter(Boolean),
        csrfToken,
      );
      if (!mountedRef.current || createEpoch !== createEpochRef.current) {
        // A close, Escape, hidden tab, or unmount raced the response. The token must not
        // survive merely because its one-time plaintext arrived after the UI was gone.
        cancelRemote(result);
        return;
      }
      deliveryRef.current = result;
      setDelivery(result);
      await onRefresh();
    } catch (error) {
      if (mountedRef.current && createEpoch === createEpochRef.current) {
        onToast({ tone: "error", message: errorMessage(error) });
      }
    } finally {
      if (mountedRef.current && createEpoch === createEpochRef.current) {
        setBusy(false);
      }
    }
  };
  const copy = async () => {
    if (!delivery) return;
    try {
      await navigator.clipboard.writeText(delivery.code);
      onToast({
        tone: "success",
        message: "Join code copied. It will not be retained after this dialog closes.",
      });
    } catch {
      onToast({
        tone: "error",
        message: "Clipboard access is unavailable. Select and copy the join code manually.",
      });
    }
  };
  return (
    <dialog aria-labelledby="join-code-title" className="dialog" ref={dialogRef}>
      {delivery ? (
        <div className="join-reveal">
          <h2 id="join-code-title">Join code created</h2>
          <p>
            This is the only display of the credential. Copy it now; closing this dialog removes it
            from the UI and cancels it on the Controller.
          </p>
          <textarea aria-label="One-time join code" autoFocus readOnly value={delivery.code} />
          <div className="dialog-actions">
            <button onClick={() => void copy()} type="button">
              Copy code
            </button>
            <button className="button-quiet" onClick={close} type="button">
              Close and remove code
            </button>
          </div>
        </div>
      ) : (
        <form onSubmit={(event) => void create(event)}>
          <h2 id="join-code-title">Create join code</h2>
          <p>Optional HTTPS endpoint hints, one authority per line.</p>
          <label className="field">
            <span>Endpoint hints</span>
            <textarea
              onChange={(event) => setHints(event.target.value)}
              placeholder="controller.example.test:7443"
              value={hints}
            />
          </label>
          <div className="dialog-actions">
            <button disabled={busy} type="submit">
              {busy ? "Creating…" : "Create one-time code"}
            </button>
            <button className="button-quiet" onClick={close} type="button">
              Cancel
            </button>
          </div>
        </form>
      )}
    </dialog>
  );
}

function NodeStatus({ node }: { readonly node: Schema["Node"] }) {
  const state = node.administrativeState === "quarantined" ? "quarantined" : node.observedState;
  const qualifier = node.observedState === "offline" ? "Last known" : node.statusReason;
  return (
    <div className="node-status">
      <span aria-hidden="true" className={`status-light status-${state}`} />
      <span>{capitalize(state)}</span>
      {qualifier ? <small>· {qualifier}</small> : null}
    </div>
  );
}
function NodeTerminalState({ node }: { readonly node: Schema["Node"] }) {
  if (node.administrativeState === "quarantined")
    return <span className="terminal-state">Safety quarantine</span>;
  if (node.administrativeState === "revoked")
    return <span className="terminal-state">Credential revoked</span>;
  return <span className="terminal-state">Reconciling</span>;
}
function RunRows({ runs }: { readonly runs: readonly Schema["Run"][] }) {
  return (
    <div className="run-list">
      {runs.map((run) => (
        <article className="run-row" key={run.id}>
          <strong>{run.state}</strong>
          <span>{run.targetId}</span>
          <span>
            {run.nodeId} · slot {run.slotIndex}
          </span>
          {run.errorCode ? <span className="run-error">{run.errorCode}</span> : null}
        </article>
      ))}
    </div>
  );
}
function Freshness({ freshness }: { readonly freshness: Schema["Freshness"] }) {
  if (freshness.state === "fresh")
    return <span>Fresh {freshness.observedAt ? formatDate(freshness.observedAt) : ""}</span>;
  if (freshness.state === "stale")
    return (
      <span>
        Last known — stale {freshness.failedAt ? formatDate(freshness.failedAt) : ""}{" "}
        {freshness.failureCode ? `(${freshness.failureCode})` : ""}
      </span>
    );
  return <span>Verification unavailable</span>;
}
function ConditionStrip({ conditions }: { readonly conditions: readonly Schema["Condition"][] }) {
  return conditions.length ? (
    <section className="condition-strip" aria-label="Controller conditions">
      {conditions.map((condition) => (
        <p key={condition.code}>
          <strong>{condition.status}</strong> · {condition.code.replaceAll("_", " ")}
        </p>
      ))}
    </section>
  ) : null;
}
function Metric({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
function Step({
  complete,
  index,
  title,
  children,
}: {
  readonly complete: boolean;
  readonly index: string;
  readonly title: string;
  readonly children: ReactNode;
}) {
  return (
    <section className="setup-step">
      <span>{index}</span>
      <div>
        <div className="step-heading">
          <h2>{title}</h2>
          <strong>{complete ? "Ready" : "Needs attention"}</strong>
        </div>
        {children}
      </div>
    </section>
  );
}
function DisabledAction({
  children,
  reason,
}: {
  readonly children: ReactNode;
  readonly reason?: string;
}) {
  const reasonId = useId();
  return (
    <span className="disabled-action">
      <button aria-describedby={reason ? reasonId : undefined} disabled type="button">
        {children}
      </button>
      {reason ? <small id={reasonId}>{reason}</small> : null}
    </span>
  );
}
function EmptyPanel({ title, detail }: { readonly title: string; readonly detail: string }) {
  return (
    <div className="empty-panel">
      <h3>{title}</h3>
      <p>{detail}</p>
    </div>
  );
}
function ProblemReference({ problem }: { readonly problem?: Problem }) {
  return problem?.requestId ? (
    <p className="request-id">
      Request ID: <code>{problem.requestId}</code>
    </p>
  ) : null;
}
function Brand() {
  return (
    <div className="brand">
      <p>LAN-first runner fleet</p>
      <strong>Tewake</strong>
    </div>
  );
}
type PageProps = {
  readonly api: ManagementClient;
  readonly csrfToken: string;
  readonly snapshot: Snapshot;
  readonly onRefresh: () => Promise<boolean>;
  readonly onToast: (value: Toast) => void;
  readonly mutationsDisabled: boolean;
};
function hashRoute(): ScreenName {
  const candidate = window.location.hash.replace(/^#\//, "") as ScreenName;
  return navigation.some((item) => item.id === candidate) ? candidate : "overview";
}
function handleReadError(
  error: unknown,
  setState: (state: LoadState) => void,
  setProblem: (problem: Problem | undefined) => void,
) {
  const apiError = apiErrorShape(error);
  if (apiError) {
    setProblem(apiError.problem);
    setState(
      apiError.status === 401 || apiError.status === 403 ? "handoff-required" : "unavailable",
    );
    return;
  }
  setState("unavailable");
}
function apiErrorShape(
  error: unknown,
): { readonly status: number; readonly problem?: Problem } | undefined {
  if (error instanceof ApiError) return error;
  if (typeof error !== "object" || error === null) return undefined;
  const candidate = error as { status?: unknown; problem?: unknown };
  return typeof candidate.status === "number"
    ? { status: candidate.status, problem: candidate.problem as Problem | undefined }
    : undefined;
}
function errorMessage(error: unknown) {
  return error instanceof ApiError
    ? error.message
    : "The requested management operation did not complete.";
}
function fieldProblem(problem: Problem | undefined, field: string) {
  return problem?.errors?.find((error) => error.field === field)?.message;
}
function isCommittedMutation(error: unknown) {
  return apiErrorShape(error)?.problem?.code === "mutation_committed_reload_required";
}
function capitalize(value: string) {
  return value.slice(0, 1).toUpperCase() + value.slice(1);
}
function formatDate(value: string) {
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(
    new Date(value),
  );
}
function formatBytes(value: string) {
  const bytes = Number(value);
  return Number.isFinite(bytes) ? `${(bytes / 1024 ** 3).toFixed(1)} GB` : "Memory not reported";
}
