// Command tewake-tray is the optional, unprivileged desktop presentation of
// this computer's Tewake node. It renders what the local Agent reports and
// toggles exactly one value: whether this computer accepts new jobs.
//
// It is not an authority. It holds no controller credential, speaks only the
// same-host node control contract, and its absence changes no fleet behavior.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"fyne.io/systray"
	"github.com/genm/sparerunner/internal/buildinfo"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/nodectl"
)

const refreshInterval = 3 * time.Second

// targetPoolSize bounds the number of per-Target menu items the tray
// pre-creates. systray items cannot be created or destroyed on the fly across
// platforms, so a fixed pool is retitled/shown/hidden on every refresh instead;
// a fleet with more eligible/excluded Targets than the pool renders a single
// disabled overflow item rather than truncating silently.
const targetPoolSize = 16

func main() {
	stateDirectory := flag.String(
		"state-dir", "", "agent state directory (default: OS user config directory)",
	)
	showVersion := flag.Bool("version", false, "print version information")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}
	directory, err := resolveStateDirectory(*stateDirectory)
	if err != nil {
		fail(err)
	}
	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceTray}
	// The endpoint path is validated before the tray host is opened so an
	// unusable configuration reports a terminal error instead of a menu bar
	// icon that can never work.
	if _, err := nodectl.EndpointPath(directory); err != nil {
		fail(err)
	}
	tray := &trayApp{client: client}
	systray.Run(tray.onReady, func() {})
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "tewake-tray: %v\n", err)
	// A desktop without a usable tray host, or an unusable endpoint, is an
	// explicit unsupported environment. The CLI remains the supported path.
	os.Exit(1)
}

func resolveStateDirectory(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve OS user configuration directory: %w", err)
	}
	return filepath.Join(config, "tewake", "agent"), nil
}

type trayApp struct {
	client nodectl.Client

	mu       sync.Mutex
	headline *systray.MenuItem
	detail   *systray.MenuItem
	running  *systray.MenuItem
	pause    *systray.MenuItem
	resume   *systray.MenuItem

	targetsHeading *systray.MenuItem
	targetPool     [targetPoolSize]*systray.MenuItem
	// targetPoolTarget records which Target ID each pool slot currently
	// renders, so a click on that slot's ClickedCh can be resolved back to an
	// action without racing a concurrent refresh. "" means the slot is hidden.
	targetPoolTarget [targetPoolSize]domain.TargetID
	targetPoolAction [targetPoolSize]func(domain.TargetID) (nodectl.Status, error)
	overflow         *systray.MenuItem

	// targetClicked receives a pool index whenever that slot's menu item is
	// clicked. One forwarding goroutine per pool slot feeds it, so the main
	// loop can select over a single channel instead of a fixed-size case list.
	targetClicked chan int
}

func (app *trayApp) onReady() {
	systray.SetTitle(titleUnknown)
	systray.SetTooltip("Tewake node")

	app.headline = systray.AddMenuItem("Checking this computer…", "")
	app.headline.Disable()
	app.detail = systray.AddMenuItem("", "")
	app.detail.Disable()
	app.running = systray.AddMenuItem("", "")
	app.running.Disable()
	systray.AddSeparator()
	app.pause = systray.AddMenuItem("Stop accepting jobs", "Withhold this computer's capacity")
	app.resume = systray.AddMenuItem("Resume accepting jobs", "Offer this computer's capacity again")
	systray.AddSeparator()
	app.targetsHeading = systray.AddMenuItem("Targets", "GitHub Targets this computer may serve")
	app.targetsHeading.Disable()
	app.targetClicked = make(chan int, targetPoolSize)
	for index := range app.targetPool {
		item := systray.AddMenuItem("", "")
		item.Hide()
		app.targetPool[index] = item
		go func(slot int, clicked chan struct{}) {
			for range clicked {
				app.targetClicked <- slot
			}
		}(index, item.ClickedCh)
	}
	app.overflow = systray.AddMenuItem("", "")
	app.overflow.Disable()
	app.overflow.Hide()
	systray.AddSeparator()
	quit := systray.AddMenuItem("Quit", "Close the tray; the agent keeps running")

	app.refresh()
	go app.loop(quit)
}

func (app *trayApp) loop(quit *systray.MenuItem) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-app.pause.ClickedCh:
			app.apply(app.client.Pause)
		case <-app.resume.ClickedCh:
			app.apply(app.client.Resume)
		case slot := <-app.targetClicked:
			app.applyTargetClick(slot)
		case <-ticker.C:
			app.refresh()
		case <-quit.ClickedCh:
			systray.Quit()
			return
		}
	}
}

// applyTargetClick resolves a pool slot's recorded action under the lock, then
// invokes it outside the lock so a slow control-endpoint round trip cannot
// stall a concurrent refresh's rendering.
func (app *trayApp) applyTargetClick(slot int) {
	app.mu.Lock()
	targetID := app.targetPoolTarget[slot]
	action := app.targetPoolAction[slot]
	app.mu.Unlock()
	if targetID == "" || action == nil {
		return
	}
	status, err := action(targetID)
	app.render(status, err)
}

func (app *trayApp) apply(action func() (nodectl.Status, error)) {
	status, err := action()
	app.render(status, err)
}

func (app *trayApp) refresh() {
	status, err := app.client.Status()
	app.render(status, err)
}

const (
	titleAccepting = "● Tewake"
	titleStopped   = "○ Tewake"
	titlePending   = "◐ Tewake"
	titleUnknown   = "⚠ Tewake"
)

// render never collapses an error, a stale observation, or a pending resume
// into the accepting appearance. Each is a distinct explicit state.
func (app *trayApp) render(status nodectl.Status, err error) {
	app.mu.Lock()
	defer app.mu.Unlock()
	if err != nil {
		systray.SetTitle(titleUnknown)
		app.headline.SetTitle("Unknown — cannot reach the local agent")
		app.detail.SetTitle(errorDetail(err))
		app.running.SetTitle("Running: unknown")
		// Without a reachable agent, neither action can be confirmed, so
		// neither is offered.
		app.pause.Disable()
		app.resume.Disable()
		app.clearTargetPool()
		return
	}
	switch {
	case !status.Intent.Accepts():
		systray.SetTitle(titleStopped)
		app.headline.SetTitle("Not accepting new jobs")
		app.pause.Disable()
		app.resume.Enable()
	case status.PendingResume:
		systray.SetTitle(titlePending)
		app.headline.SetTitle("Resume pending — controller has not confirmed")
		app.pause.Enable()
		app.resume.Disable()
	case !status.NativeRunnerReady:
		systray.SetTitle(titlePending)
		app.headline.SetTitle("Accepting, but the native runner is unavailable")
		app.pause.Enable()
		app.resume.Disable()
	default:
		systray.SetTitle(titleAccepting)
		app.headline.SetTitle("Accepting new jobs")
		app.pause.Enable()
		app.resume.Disable()
	}
	app.detail.SetTitle(fmt.Sprintf(
		"Node %s · controller %s%s",
		shortNode(string(status.NodeID)),
		connectionText(status.ControllerConnected),
		isolationDetail(status),
	))
	// The tooltip carries the long form: the menu row is width-constrained, and
	// a desktop user must be able to read what the weaker mode actually drops
	// rather than a shorthand they have to look up.
	systray.SetTooltip(isolationTooltip(status))
	app.running.SetTitle(runningText(status))
	app.renderTargetPool(status)
}

// isolationDetail appends the weaker runner-identity mode to the detail row.
// The isolated mode renders nothing: it is the expectation, and only the drop
// needs to be visible so nobody mistakes one mode for the other.
func isolationDetail(status nodectl.Status) string {
	if !status.SharedRunnerIdentity {
		return ""
	}
	return " · shared runner identity"
}

func isolationTooltip(status nodectl.Status) string {
	if !status.SharedRunnerIdentity {
		return "Tewake node"
	}
	return "Tewake node — shared runner identity: jobs run as the agent user, " +
		"without UID isolation"
}

// targetPoolEntry is one row the pool can render: a scoped eligible Target or
// an unknown locally-excluded Target ID. label and action are computed once so
// renderTargetPool never mixes species of entry into the same slot.
type targetPoolEntry struct {
	targetID domain.TargetID
	label    string
	action   func(domain.TargetID) (nodectl.Status, error)
}

// renderTargetPool must be called with app.mu already held. It retitles a
// fixed pool of pre-created menu items rather than creating or destroying
// items on the fly, because systray menus cannot do that reliably across
// platforms. Entries beyond the pool collapse into a single disabled overflow
// item pointing at the CLI rather than being silently dropped.
func (app *trayApp) renderTargetPool(status nodectl.Status) {
	entries := make([]targetPoolEntry, 0, len(status.Targets())+len(status.UnknownExclusions))
	for _, target := range status.Targets() {
		entries = append(entries, targetPoolEntry{
			targetID: target.TargetID,
			label:    fmt.Sprintf("%s — %s", target.Scope, targetStateLabel(target)),
			action:   targetToggleAction(app.client, target.LocallyExcluded),
		})
	}
	for _, targetID := range status.UnknownExclusions {
		entries = append(entries, targetPoolEntry{
			targetID: targetID,
			label:    fmt.Sprintf("%s — not currently eligible", targetID),
			action:   app.client.Include,
		})
	}
	shown := entries
	overflow := 0
	if len(entries) > targetPoolSize {
		shown = entries[:targetPoolSize]
		overflow = len(entries) - targetPoolSize
	}
	for slot, item := range app.targetPool {
		if slot >= len(shown) {
			item.Hide()
			app.targetPoolTarget[slot] = ""
			app.targetPoolAction[slot] = nil
			continue
		}
		entry := shown[slot]
		item.SetTitle(entry.label)
		item.Show()
		item.Enable()
		app.targetPoolTarget[slot] = entry.targetID
		app.targetPoolAction[slot] = entry.action
	}
	if overflow > 0 {
		app.overflow.SetTitle(fmt.Sprintf("+%d more (use CLI)", overflow))
		app.overflow.Show()
	} else {
		app.overflow.Hide()
	}
}

// clearTargetPool hides every pool slot when the agent is unreachable, so a
// stale Target list is never rendered alongside an unknown top-level state.
// Callers must already hold app.mu.
func (app *trayApp) clearTargetPool() {
	for slot, item := range app.targetPool {
		item.Hide()
		app.targetPoolTarget[slot] = ""
		app.targetPoolAction[slot] = nil
	}
	app.overflow.Hide()
}

// targetStateLabel names the four distinct owner-facing states a heartbeat can
// report: serving, adopted-excluded, an owner exclusion the controller has not
// yet adopted, and an owner inclusion the controller has not yet adopted. The
// last never renders as served, matching the additive/subtractive asymmetry.
func targetStateLabel(target nodectl.EligibleTarget) string {
	switch {
	case !target.LocallyExcluded && !target.Excluded:
		return "serving"
	case target.LocallyExcluded && target.Excluded:
		return "excluded"
	case target.LocallyExcluded && !target.Excluded:
		return "excluded — syncing"
	default: // !target.LocallyExcluded && target.Excluded
		return "include pending"
	}
}

// targetToggleAction picks the mutation a click on this row should perform:
// a currently-excluded row (whether adopted or still syncing) offers Include,
// and everything else offers Exclude.
func targetToggleAction(
	client nodectl.Client,
	locallyExcluded bool,
) func(domain.TargetID) (nodectl.Status, error) {
	if locallyExcluded {
		return client.Include
	}
	return client.Exclude
}

func errorDetail(err error) string {
	var controlErr *nodectl.Error
	if errors.As(err, &controlErr) {
		switch controlErr.Class {
		case nodectl.ErrorClassEndpointUnavailable:
			return "The agent service is not running or has no local control endpoint"
		case nodectl.ErrorClassUnauthorizedPeer:
			return "This desktop account is not an authorized node owner"
		case nodectl.ErrorClassProtocolMismatch:
			return "The agent speaks a different control protocol version"
		case nodectl.ErrorClassEndpointUnsupported:
			return "Local control is unsupported on this platform"
		}
		return controlErr.Class
	}
	return err.Error()
}

func runningText(status nodectl.Status) string {
	if len(status.RunningExecutions) == 0 {
		return "Running: none"
	}
	return fmt.Sprintf("Running: %d execution(s)", len(status.RunningExecutions))
}

func connectionText(connected bool) string {
	if connected {
		return "connected"
	}
	return "disconnected"
}

func shortNode(nodeID string) string {
	if len(nodeID) <= 12 {
		return nodeID
	}
	return nodeID[:12] + "…"
}
