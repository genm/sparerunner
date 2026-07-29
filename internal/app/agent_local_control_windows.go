//go:build windows

package app

import (
	"github.com/genm/sparerunner/internal/nodectl"
)

// localControlPrincipals authorizes the service account plus every explicitly
// named owner SID. The same list becomes the pipe DACL and the per-connection
// allowlist, so an account can never be reachable without also being authorized,
// or authorized without being reachable.
func localControlPrincipals(
	options AgentLocalControlOptions,
) ([]string, nodectl.Authorizer, error) {
	self, err := nodectl.ServiceAccountSID()
	if err != nil {
		return nil, nil, err
	}
	principals := append([]string{self}, options.OwnerSIDs...)
	return principals, nodectl.NewSIDAllowlist(principals...), nil
}
