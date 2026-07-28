package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/genm/sparerunner/internal/api/gen"
)

// generatedServerContract makes the OpenAPI operation set a compile-time
// dependency of the hand-written security adapter. The generated router is not
// used because it binds required CSRF headers before session authentication,
// which would violate the API's deliberate authentication-first error order.
// The runtime parity test separately proves that every generated method/path is
// dispatched by server.route.
type generatedServerContract struct {
	server    *server
	requestID string
}

var _ gen.ServerInterface = (*generatedServerContract)(nil)

func (contract *generatedServerContract) AuthorizeBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.AuthorizeBrowserHandoffParams,
) {
	contract.server.authorizeBrowserHandoff(
		writer,
		request,
		contract.requestID,
	)
}

func (contract *generatedServerContract) CreateBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.createBrowserHandoff(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ClaimBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.claimBrowserHandoff(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ListAuditEvents(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.ListAuditEventsParams,
) {
	// Bind query parameters inside the handler, after authentication, so an
	// invalid cursor cannot be used to distinguish whether a session exists.
	contract.server.listAuditEvents(writer, request, contract.requestID)
}

func (contract *generatedServerContract) GetConfiguration(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.getConfiguration(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ApplyConfiguration(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.ApplyConfigurationParams,
) {
	contract.server.applyConfiguration(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ExportConfiguration(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.exportConfiguration(writer, request, contract.requestID)
}

func (contract *generatedServerContract) StreamEvents(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.StreamEventsParams,
) {
	contract.server.streamEvents(writer, request, contract.requestID)
}

func (contract *generatedServerContract) CreateJoinCode(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.CreateJoinCodeParams,
) {
	contract.server.createJoinCode(writer, request, contract.requestID)
}

func (contract *generatedServerContract) CancelJoinCode(
	writer http.ResponseWriter,
	request *http.Request,
	_ string,
	_ gen.CancelJoinCodeParams,
) {
	contract.server.cancelJoinCode(
		writer,
		request,
		contract.requestID,
		requestPathTail(request, "/join-codes/"),
	)
}

func (contract *generatedServerContract) ListNodes(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.listNodes(writer, request, contract.requestID)
}

func (contract *generatedServerContract) DrainNode(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.NodeID,
	_ gen.DrainNodeParams,
) {
	contract.server.mutateNode(writer, request, contract.requestID, apiRelativePath(request))
}

func (contract *generatedServerContract) ResumeNode(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.NodeID,
	_ gen.ResumeNodeParams,
) {
	contract.server.mutateNode(writer, request, contract.requestID, apiRelativePath(request))
}

func (contract *generatedServerContract) RevokeNode(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.NodeID,
	_ gen.RevokeNodeParams,
) {
	contract.server.mutateNode(writer, request, contract.requestID, apiRelativePath(request))
}

func (contract *generatedServerContract) GetOverview(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.read(
		writer,
		request,
		contract.requestID,
		func(ctx context.Context) (any, error) {
			return contract.server.backend.Overview(ctx)
		},
	)
}

func (contract *generatedServerContract) ListRuns(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.listRuns(writer, request, contract.requestID)
}

func (contract *generatedServerContract) DeleteSession(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.DeleteSessionParams,
) {
	contract.server.deleteSession(writer, request, contract.requestID)
}

func (contract *generatedServerContract) GetSession(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.getSession(writer, request, contract.requestID)
}

func (contract *generatedServerContract) CreateSession(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.CreateSessionParams,
) {
	contract.server.createSession(writer, request, contract.requestID)
}

func (contract *generatedServerContract) GetSetup(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.read(
		writer,
		request,
		contract.requestID,
		func(ctx context.Context) (any, error) {
			return contract.server.backend.Setup(ctx)
		},
	)
}

func (contract *generatedServerContract) StartGitHubAppManifest(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.StartGitHubAppManifestParams,
) {
	contract.server.startGitHubAppManifest(writer, request, contract.requestID)
}

func (contract *generatedServerContract) CompleteGitHubAppManifest(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.CompleteGitHubAppManifestParams,
) {
	contract.server.completeGitHubAppManifest(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ListGitHubInstallations(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.listGitHubInstallations(writer, request, contract.requestID)
}

func (contract *generatedServerContract) CreateGitHubTarget(
	writer http.ResponseWriter,
	request *http.Request,
	_ gen.CreateGitHubTargetParams,
) {
	contract.server.createGitHubTarget(writer, request, contract.requestID)
}

func (contract *generatedServerContract) ListTargets(
	writer http.ResponseWriter,
	request *http.Request,
) {
	contract.server.listTargets(writer, request, contract.requestID)
}

func apiRelativePath(request *http.Request) string {
	if request == nil || request.URL == nil {
		return ""
	}
	return requestPathTail(request, "")
}

func requestPathTail(request *http.Request, prefix string) string {
	if request == nil || request.URL == nil {
		return ""
	}
	path := request.URL.Path
	if relative, found := strings.CutPrefix(path, Prefix); found {
		path = relative
	}
	return strings.TrimPrefix(path, prefix)
}
