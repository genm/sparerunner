package app

import (
	"log/slog"

	"github.com/genm/sparerunner/internal/nodectl"
)

// AgentLocalControlOptions configures the same-host endpoint the tray, the
// launcher, and the CLI use. Authorization is an explicit allowlist of local
// principals: the service account itself, plus any node-owner desktop account
// the operator names. The naming is per platform because the two systems have no
// common principal type — OwnerUIDs on Unix, OwnerSIDs on Windows — and the
// option that does not apply to the running platform is ignored. An empty
// allowlist authorizes nobody but the service account.
type AgentLocalControlOptions struct {
	Enabled   bool
	OwnerUIDs []int
	OwnerSIDs []string
	Logger    *slog.Logger
}

func startAgentLocalControl(
	stateDirectory string,
	availability *agentAvailability,
	options AgentLocalControlOptions,
	logger *slog.Logger,
) (*nodectl.Server, error) {
	// The principal list is resolved before the endpoint exists so a
	// misconfigured owner fails startup instead of producing a listening
	// endpoint that silently authorizes fewer accounts than the operator asked
	// for.
	principals, authorizer, err := localControlPrincipals(options)
	if err != nil {
		return nil, err
	}
	listener, err := nodectl.Listen(stateDirectory, principals)
	if err != nil {
		return nil, err
	}
	controlLogger := options.Logger
	if controlLogger == nil {
		controlLogger = logger
	}
	server, err := nodectl.Serve(listener, nodectl.ServerOptions{
		Controller: availability,
		Authorizer: authorizer,
		Logger:     controlLogger,
	})
	if err != nil {
		listener.Close()
		return nil, err
	}
	controlLogger.Info(
		"node control endpoint listening",
		"component", "nodectl",
		"authorized_principal_count", len(principals),
	)
	return server, nil
}
