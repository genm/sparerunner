//go:build windows

package windows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/genm/sparerunner/internal/enroll"
	syswindows "golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	BootstrapPipeName    = `\\.\pipe\TewakeEnroll`
	AgentServiceName     = "TewakeAgent"
	bootstrapProtocolV1  = 1
	maxBootstrapFrame    = 64 << 10
	pipeAccessDuplex     = 0x00000003
	bootstrapPollDelay   = 25 * time.Millisecond
	bootstrapErrorFailed = "enrollment_failed"
	bootstrapPipeMode    = syswindows.PIPE_TYPE_MESSAGE |
		syswindows.PIPE_READMODE_MESSAGE |
		syswindows.PIPE_WAIT |
		syswindows.PIPE_REJECT_REMOTE_CLIENTS
)

var (
	ErrBootstrapProtocol    = errors.New("invalid Windows enrollment bootstrap protocol")
	ErrBootstrapIdentity    = errors.New("Windows enrollment bootstrap identity mismatch")
	ErrBootstrapUnavailable = errors.New("Windows enrollment bootstrap unavailable")
	ErrBootstrapEnrollment  = errors.New("Windows service enrollment failed")
)

func bootstrapIdentityError(reason string) error {
	if os.Getenv("TEWAKE_WINDOWS_DEBUG") == "1" {
		return fmt.Errorf("%w: %s", ErrBootstrapIdentity, reason)
	}
	return ErrBootstrapIdentity
}

// BootstrapJoinOptions is the one request accepted by the service bootstrap
// pipe. StateDirectory is deliberately absent: the SCM-owned service config is
// the only authority for where DPAPI-protected node state is written.
type BootstrapJoinOptions struct {
	JoinCode          string
	Controller        string
	DiscoveryTimeout  time.Duration
	ConnectionTimeout time.Duration
}

type bootstrapRequestFrame struct {
	Version                int    `json:"version"`
	JoinCode               string `json:"joinCode"`
	Controller             string `json:"controller,omitempty"`
	DiscoveryTimeoutNanos  int64  `json:"discoveryTimeoutNanos"`
	ConnectionTimeoutNanos int64  `json:"connectionTimeoutNanos"`
}

type bootstrapResponseFrame struct {
	Version int    `json:"version"`
	OK      bool   `json:"ok"`
	NodeID  string `json:"nodeId,omitempty"`
	Code    string `json:"code,omitempty"`
}

// BootstrapRequest keeps the authenticated local pipe open until enrollment
// state is durable. Complete is the only way the CLI can observe success.
type BootstrapRequest struct {
	Options BootstrapJoinOptions

	file         *os.File
	once         sync.Once
	disconnected chan struct{}
	finished     chan struct{}
	monitorDone  chan struct{}
}

// ReceiveBootstrapRequest accepts one local elevated client. Production calls
// require the receiver to be the LocalSystem TewakeAgent service so an
// installer or interactive account can never become DPAPI authority.
func ReceiveBootstrapRequest(ctx context.Context) (*BootstrapRequest, error) {
	return receiveBootstrapRequest(ctx, true)
}

func receiveBootstrapRequest(
	ctx context.Context,
	requireSystem bool,
) (*BootstrapRequest, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrBootstrapUnavailable
	}
	if requireSystem {
		sid, err := currentProcessSID()
		if err != nil || !strings.EqualFold(sid, "S-1-5-18") {
			return nil, ErrBootstrapIdentity
		}
	}
	name, err := syswindows.UTF16PtrFromString(BootstrapPipeName)
	if err != nil {
		return nil, ErrBootstrapUnavailable
	}
	owner, err := currentProcessSID()
	if err != nil {
		return nil, ErrBootstrapIdentity
	}
	descriptor, err := syswindows.SecurityDescriptorFromString(
		fmt.Sprintf("O:%sD:P(A;;GA;;;SY)(A;;GRGW;;;BA)", owner),
	)
	if err != nil {
		return nil, ErrBootstrapUnavailable
	}
	attributes := &syswindows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(syswindows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	handle, err := syswindows.CreateNamedPipe(
		name,
		pipeAccessDuplex|syswindows.FILE_FLAG_FIRST_PIPE_INSTANCE,
		bootstrapPipeMode,
		1,
		maxBootstrapFrame,
		maxBootstrapFrame,
		0,
		attributes,
	)
	if err != nil {
		return nil, ErrBootstrapUnavailable
	}
	file := os.NewFile(uintptr(handle), BootstrapPipeName)
	if file == nil {
		syswindows.CloseHandle(handle)
		return nil, ErrBootstrapUnavailable
	}
	if err := validateBootstrapPipeACL(handle); err != nil {
		_ = file.Close()
		return nil, err
	}
	connected := make(chan error, 1)
	go func() {
		connectErr := syswindows.ConnectNamedPipe(handle, nil)
		if errors.Is(connectErr, syswindows.ERROR_PIPE_CONNECTED) {
			connectErr = nil
		}
		connected <- connectErr
	}()
	select {
	case connectErr := <-connected:
		if connectErr != nil {
			_ = file.Close()
			return nil, ErrBootstrapUnavailable
		}
	case <-ctx.Done():
		_ = file.Close()
		<-connected
		return nil, ErrBootstrapUnavailable
	}
	if err := validateBootstrapClient(handle); err != nil {
		_ = file.Close()
		return nil, err
	}
	payload, err := readPipeMessage(ctx, file, handle, maxBootstrapFrame)
	if err != nil {
		_ = writeBootstrapResponse(file, bootstrapResponseFrame{
			Version: bootstrapProtocolV1,
			Code:    "protocol_error",
		})
		_ = file.Close()
		return nil, err
	}
	defer clear(payload)
	var frame bootstrapRequestFrame
	if err := decodeBootstrapJSON(payload, &frame); err != nil ||
		validateBootstrapRequestFrame(frame) != nil {
		_ = writeBootstrapResponse(file, bootstrapResponseFrame{
			Version: bootstrapProtocolV1,
			Code:    "protocol_error",
		})
		_ = file.Close()
		return nil, ErrBootstrapProtocol
	}
	request := &BootstrapRequest{
		Options: BootstrapJoinOptions{
			JoinCode:          frame.JoinCode,
			Controller:        frame.Controller,
			DiscoveryTimeout:  time.Duration(frame.DiscoveryTimeoutNanos),
			ConnectionTimeout: time.Duration(frame.ConnectionTimeoutNanos),
		},
		file:         file,
		disconnected: make(chan struct{}),
		finished:     make(chan struct{}),
		monitorDone:  make(chan struct{}),
	}
	go request.monitorClient()
	return request, nil
}

func (request *BootstrapRequest) Disconnected() <-chan struct{} {
	if request == nil || request.disconnected == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return request.disconnected
}

// JoinOptions returns a copy so the service wrapper can consume bootstrap
// requests through a narrow interface without exposing transport internals.
func (request *BootstrapRequest) JoinOptions() BootstrapJoinOptions {
	if request == nil {
		return BootstrapJoinOptions{}
	}
	return request.Options
}

// Complete sends only a fixed error class on failure. It never serializes an
// enrollment error, join code, controller response, or credential material.
func (request *BootstrapRequest) Complete(nodeID string, enrollmentErr error) error {
	if request == nil || request.file == nil {
		return ErrBootstrapProtocol
	}
	var completeErr error = ErrBootstrapProtocol
	request.once.Do(func() {
		close(request.finished)
		defer func() {
			request.Options.JoinCode = ""
			if err := request.file.Close(); completeErr == nil && err != nil {
				completeErr = ErrBootstrapUnavailable
			}
			<-request.monitorDone
		}()
		response := bootstrapResponseFrame{
			Version: bootstrapProtocolV1,
			OK:      enrollmentErr == nil,
			NodeID:  nodeID,
		}
		if enrollmentErr != nil {
			response.NodeID = ""
			response.Code = bootstrapErrorFailed
		}
		if err := validateBootstrapResponseFrame(response); err != nil {
			completeErr = err
			return
		}
		if err := writeBootstrapResponse(request.file, response); err != nil {
			completeErr = ErrBootstrapUnavailable
			return
		}
		completeErr = nil
	})
	return completeErr
}

func (request *BootstrapRequest) monitorClient() {
	defer close(request.monitorDone)
	var extra [1]byte
	_, _ = request.file.Read(extra[:])
	select {
	case <-request.finished:
		return
	default:
		close(request.disconnected)
	}
}

// SubmitBootstrapJoin sends the capability only after verifying that the pipe
// server PID is the running LocalSystem TewakeAgent SCM service.
func SubmitBootstrapJoin(
	ctx context.Context,
	options BootstrapJoinOptions,
) (string, error) {
	return submitBootstrapJoin(ctx, options, true)
}

func submitBootstrapJoin(
	ctx context.Context,
	options BootstrapJoinOptions,
	verifyServer bool,
) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrBootstrapUnavailable
	}
	frame := bootstrapRequestFrame{
		Version:                bootstrapProtocolV1,
		JoinCode:               options.JoinCode,
		Controller:             options.Controller,
		DiscoveryTimeoutNanos:  int64(options.DiscoveryTimeout),
		ConnectionTimeoutNanos: int64(options.ConnectionTimeout),
	}
	if err := validateBootstrapRequestFrame(frame); err != nil {
		return "", err
	}
	payload, err := encodeBootstrapJSON(frame)
	if err != nil {
		return "", err
	}
	defer clear(payload)
	connectTimeout := options.ConnectionTimeout
	if connectTimeout <= 0 {
		connectTimeout = 30 * time.Second
	}
	connectContext, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	file, handle, err := connectBootstrapPipe(connectContext)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if verifyServer {
		if err := validateBootstrapServer(handle); err != nil {
			return "", err
		}
	}
	if _, err := file.Write(payload); err != nil {
		return "", ErrBootstrapUnavailable
	}
	responsePayload, err := readPipeMessage(
		connectContext,
		file,
		handle,
		maxBootstrapFrame,
	)
	if err != nil {
		return "", err
	}
	defer clear(responsePayload)
	var response bootstrapResponseFrame
	if err := decodeBootstrapJSON(responsePayload, &response); err != nil ||
		validateBootstrapResponseFrame(response) != nil {
		return "", ErrBootstrapProtocol
	}
	if !response.OK {
		return "", ErrBootstrapEnrollment
	}
	return response.NodeID, nil
}

func connectBootstrapPipe(ctx context.Context) (*os.File, syswindows.Handle, error) {
	name, err := syswindows.UTF16PtrFromString(BootstrapPipeName)
	if err != nil {
		return nil, 0, ErrBootstrapUnavailable
	}
	for {
		handle, openErr := syswindows.CreateFile(
			name,
			syswindows.GENERIC_READ|syswindows.GENERIC_WRITE,
			0,
			nil,
			syswindows.OPEN_EXISTING,
			syswindows.SECURITY_SQOS_PRESENT|syswindows.SECURITY_IDENTIFICATION,
			0,
		)
		if openErr == nil {
			file := os.NewFile(uintptr(handle), BootstrapPipeName)
			if file == nil {
				syswindows.CloseHandle(handle)
				return nil, 0, ErrBootstrapUnavailable
			}
			mode := uint32(syswindows.PIPE_READMODE_MESSAGE)
			if err := syswindows.SetNamedPipeHandleState(
				handle,
				&mode,
				nil,
				nil,
			); err != nil {
				_ = file.Close()
				return nil, 0, ErrBootstrapUnavailable
			}
			return file, handle, nil
		}
		if !errors.Is(openErr, syswindows.ERROR_PIPE_BUSY) &&
			!errors.Is(openErr, syswindows.ERROR_FILE_NOT_FOUND) {
			return nil, 0, ErrBootstrapUnavailable
		}
		timer := time.NewTimer(bootstrapPollDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, 0, ErrBootstrapUnavailable
		case <-timer.C:
		}
	}
}

func readPipeMessage(
	ctx context.Context,
	file *os.File,
	handle syswindows.Handle,
	maximum int,
) ([]byte, error) {
	if ctx == nil || file == nil || maximum <= 0 {
		return nil, ErrBootstrapProtocol
	}
	result := make(chan struct {
		value []byte
		err   error
	}, 1)
	go func() {
		var value []byte
		buffer := make([]byte, 4<<10)
		defer clear(buffer)
		for {
			var read uint32
			err := syswindows.ReadFile(handle, buffer, &read, nil)
			if int(read) > maximum-len(value) {
				clear(value)
				result <- struct {
					value []byte
					err   error
				}{err: ErrBootstrapProtocol}
				return
			}
			if read > 0 {
				value = append(value, buffer[:read]...)
			}
			switch {
			case err == nil:
				result <- struct {
					value []byte
					err   error
				}{value: value}
				return
			case errors.Is(err, syswindows.ERROR_MORE_DATA):
				continue
			default:
				clear(value)
				result <- struct {
					value []byte
					err   error
				}{err: ErrBootstrapUnavailable}
				return
			}
		}
	}()
	select {
	case received := <-result:
		return received.value, received.err
	case <-ctx.Done():
		_ = file.Close()
		received := <-result
		clear(received.value)
		return nil, ErrBootstrapUnavailable
	}
}

func writeBootstrapResponse(file *os.File, response bootstrapResponseFrame) error {
	payload, err := encodeBootstrapJSON(response)
	if err != nil {
		return err
	}
	defer clear(payload)
	written, err := file.Write(payload)
	if err != nil || written != len(payload) {
		return ErrBootstrapUnavailable
	}
	return nil
}

func encodeBootstrapJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil || len(payload) == 0 || len(payload) > maxBootstrapFrame {
		clear(payload)
		return nil, ErrBootstrapProtocol
	}
	return payload, nil
}

func decodeBootstrapJSON(payload []byte, destination any) error {
	if len(payload) == 0 || len(payload) > maxBootstrapFrame {
		return ErrBootstrapProtocol
	}
	if err := rejectDuplicateBootstrapJSONKeys(payload); err != nil {
		return ErrBootstrapProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return ErrBootstrapProtocol
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrBootstrapProtocol
	}
	return nil
}

func validateBootstrapRequestFrame(frame bootstrapRequestFrame) error {
	if frame.Version != bootstrapProtocolV1 ||
		frame.JoinCode == "" ||
		frame.DiscoveryTimeoutNanos < 0 ||
		frame.ConnectionTimeoutNanos < 0 {
		return ErrBootstrapProtocol
	}
	if _, err := enroll.DecodeJoinCode(frame.JoinCode); err != nil {
		return ErrBootstrapProtocol
	}
	return nil
}

func validateBootstrapResponseFrame(frame bootstrapResponseFrame) error {
	if frame.Version != bootstrapProtocolV1 {
		return ErrBootstrapProtocol
	}
	if frame.OK {
		if !canonicalToken(frame.NodeID) || frame.Code != "" {
			return ErrBootstrapProtocol
		}
		return nil
	}
	if frame.NodeID != "" || frame.Code != bootstrapErrorFailed && frame.Code != "protocol_error" {
		return ErrBootstrapProtocol
	}
	return nil
}

func validateBootstrapPipeACL(handle syswindows.Handle) error {
	descriptor, err := syswindows.GetSecurityInfo(
		handle,
		// Named pipes are file objects. Using the kernel-object selector can
		// reject an otherwise valid descriptor on Windows hosted runners before
		// the client ever gets a chance to connect.
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION|syswindows.DACL_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil || !descriptor.IsValid() {
		return bootstrapIdentityError(fmt.Sprintf("security descriptor err=%v", err))
	}
	owner, defaulted, err := descriptor.Owner()
	current, currentErr := currentProcessSID()
	if err != nil || owner == nil || defaulted || currentErr != nil ||
		!strings.EqualFold(owner.String(), current) {
		return bootstrapIdentityError(fmt.Sprintf("owner owner=%v current=%q defaulted=%t err=%v currentErr=%v", owner, current, defaulted, err, currentErr))
	}
	control, _, err := descriptor.Control()
	if err != nil || control&syswindows.SE_DACL_PROTECTED == 0 {
		return bootstrapIdentityError(fmt.Sprintf("dacl protection control=0x%x err=%v", control, err))
	}
	dacl, defaulted, err := descriptor.DACL()
	var aceCount uint16
	if dacl != nil {
		aceCount = dacl.AceCount
	}
	if err != nil || dacl == nil || defaulted || dacl.AceCount != 2 {
		return bootstrapIdentityError(fmt.Sprintf("dacl count=%d defaulted=%t err=%v", aceCount, defaulted, err))
	}
	expected := map[string]syswindows.ACCESS_MASK{
		"S-1-5-18":     0x001f01ff,
		"S-1-5-32-544": 0x0012019f,
	}
	seen := map[string]bool{}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *syswindows.ACCESS_ALLOWED_ACE
		if err := syswindows.GetAce(dacl, index, &ace); err != nil ||
			ace == nil ||
			ace.Header.AceType != syswindows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags != 0 {
			return bootstrapIdentityError(fmt.Sprintf("ace %d header err=%v ace=%v", index, err, ace))
		}
		sid := (*syswindows.SID)(unsafe.Pointer(&ace.SidStart))
		if sid == nil || !sid.IsValid() {
			return bootstrapIdentityError(fmt.Sprintf("ace %d sid", index))
		}
		text := strings.ToUpper(sid.String())
		mask, found := expected[text]
		if !found || seen[text] || ace.Mask != mask {
			return bootstrapIdentityError(fmt.Sprintf("ace %d sid=%s mask=0x%x expected=0x%x found=%t duplicate=%t", index, text, ace.Mask, mask, found, seen[text]))
		}
		seen[text] = true
	}
	if !seen["S-1-5-18"] || !seen["S-1-5-32-544"] {
		return bootstrapIdentityError(fmt.Sprintf("sid set system=%t administrators=%t", seen["S-1-5-18"], seen["S-1-5-32-544"]))
	}
	return nil
}

func validateBootstrapClient(handle syswindows.Handle) error {
	var processID uint32
	if err := syswindows.GetNamedPipeClientProcessId(handle, &processID); err != nil ||
		processID == 0 {
		return bootstrapIdentityError(fmt.Sprintf("client process id=%d err=%v", processID, err))
	}
	return validateElevatedProcess(processID)
}

func validateBootstrapServer(handle syswindows.Handle) error {
	var processID uint32
	if err := syswindows.GetNamedPipeServerProcessId(handle, &processID); err != nil ||
		processID == 0 {
		return ErrBootstrapIdentity
	}
	manager, err := mgr.Connect()
	if err != nil {
		return ErrBootstrapIdentity
	}
	defer manager.Disconnect()
	service, err := manager.OpenService(AgentServiceName)
	if err != nil {
		return ErrBootstrapIdentity
	}
	defer service.Close()
	status, err := service.Query()
	if err != nil || status.State != svc.Running || status.ProcessId != processID {
		return ErrBootstrapIdentity
	}
	process, err := syswindows.OpenProcess(
		syswindows.PROCESS_QUERY_LIMITED_INFORMATION,
		false,
		processID,
	)
	if err != nil {
		return ErrBootstrapIdentity
	}
	defer syswindows.CloseHandle(process)
	var token syswindows.Token
	if err := syswindows.OpenProcessToken(process, syswindows.TOKEN_QUERY, &token); err != nil {
		return ErrBootstrapIdentity
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil ||
		!strings.EqualFold(user.User.Sid.String(), "S-1-5-18") {
		return ErrBootstrapIdentity
	}
	return nil
}

func validateElevatedProcess(processID uint32) error {
	process, err := syswindows.OpenProcess(
		syswindows.PROCESS_QUERY_LIMITED_INFORMATION,
		false,
		processID,
	)
	if err != nil {
		return bootstrapIdentityError(fmt.Sprintf("open elevated process id=%d err=%v", processID, err))
	}
	defer syswindows.CloseHandle(process)
	var token syswindows.Token
	if err := syswindows.OpenProcessToken(process, syswindows.TOKEN_QUERY, &token); err != nil {
		return bootstrapIdentityError(fmt.Sprintf("open elevated token id=%d err=%v", processID, err))
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return bootstrapIdentityError(fmt.Sprintf("elevated token user id=%d err=%v", processID, err))
	}
	if strings.EqualFold(user.User.Sid.String(), "S-1-5-18") {
		return nil
	}
	administrators, err := syswindows.CreateWellKnownSid(syswindows.WinBuiltinAdministratorsSid)
	if err != nil {
		return bootstrapIdentityError(fmt.Sprintf("administrator sid id=%d err=%v", processID, err))
	}
	member, err := tokenHasEnabledGroup(token, administrators)
	if err != nil || !member || !token.IsElevated() {
		return bootstrapIdentityError(fmt.Sprintf("elevated membership id=%d member=%t elevated=%t err=%v", processID, member, token.IsElevated(), err))
	}
	return nil
}

// tokenHasEnabledGroup reads the primary token directly. CheckTokenMembership
// requires an impersonation token when a non-zero handle is supplied, while a
// process token is exactly the authority we need to inspect here.
func tokenHasEnabledGroup(token syswindows.Token, expected *syswindows.SID) (bool, error) {
	groups, err := token.GetTokenGroups()
	if err != nil {
		return false, err
	}
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && syswindows.EqualSid(group.Sid, expected) &&
			group.Attributes&syswindows.SE_GROUP_ENABLED != 0 &&
			group.Attributes&syswindows.SE_GROUP_USE_FOR_DENY_ONLY == 0 {
			return true, nil
		}
	}
	return false, nil
}

func rejectDuplicateBootstrapJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := consumeBootstrapJSONValue(decoder, 0); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrBootstrapProtocol
	}
	return nil
}

func consumeBootstrapJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrBootstrapProtocol
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			key, ok := keyToken.(string)
			if keyErr != nil || !ok {
				return ErrBootstrapProtocol
			}
			if _, found := seen[key]; found {
				return ErrBootstrapProtocol
			}
			seen[key] = struct{}{}
			if err := consumeBootstrapJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrBootstrapProtocol
		}
	case '[':
		for decoder.More() {
			if err := consumeBootstrapJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrBootstrapProtocol
		}
	default:
		return ErrBootstrapProtocol
	}
	return nil
}
