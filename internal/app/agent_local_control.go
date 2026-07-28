package app

import (
	"log/slog"
	"os"

	"github.com/genm/sparerunner/internal/nodectl"
)

// AgentLocalControlOptions configures the same-host endpoint the tray, the
// launcher, and the CLI use. Authorization is an explicit allowlist of local
// user IDs: the service account itself, plus any node-owner desktop account the
// operator names. An empty allowlist authorizes nobody.
type AgentLocalControlOptions struct {
	Enabled   bool
	OwnerUIDs []int
	Logger    *slog.Logger
}

func startAgentLocalControl(
	stateDirectory string,
	availability *agentAvailability,
	options AgentLocalControlOptions,
	logger *slog.Logger,
) (*nodectl.Server, error) {
	listener, err := nodectl.Listen(stateDirectory)
	if err != nil {
		return nil, err
	}
	// The service account always reaches its own endpoint; extra desktop owners
	// are additive and explicit.
	uids := append([]int{os.Geteuid()}, options.OwnerUIDs...)
	controlLogger := options.Logger
	if controlLogger == nil {
		controlLogger = logger
	}
	server, err := nodectl.Serve(listener, nodectl.ServerOptions{
		Controller: availability,
		Authorizer: nodectl.NewUIDAllowlist(uids...),
		Logger:     controlLogger,
	})
	if err != nil {
		listener.Close()
		return nil, err
	}
	controlLogger.Info(
		"node control endpoint listening",
		"component", "nodectl",
		"authorized_uid_count", len(uids),
	)
	return server, nil
}
