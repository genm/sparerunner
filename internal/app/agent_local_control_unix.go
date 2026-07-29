//go:build unix

package app

import (
	"os"
	"strconv"

	"github.com/genm/sparerunner/internal/nodectl"
)

// localControlPrincipals authorizes the service account plus every explicitly
// named owner UID. The service account is first because it always reaches its
// own endpoint; extra desktop owners are additive and explicit.
func localControlPrincipals(
	options AgentLocalControlOptions,
) ([]string, nodectl.Authorizer, error) {
	uids := append([]int{os.Geteuid()}, options.OwnerUIDs...)
	principals := make([]string, 0, len(uids))
	for _, uid := range uids {
		principals = append(principals, strconv.Itoa(uid))
	}
	return principals, nodectl.NewUIDAllowlist(uids...), nil
}
