//go:build windows

package windows

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/genm/tewake/internal/enroll"
	syswindows "golang.org/x/sys/windows"
)

func TestBootstrapPipeCompletesOnlyAfterDurableEnrollmentAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	const nodeID = "0123456789abcdef0123456789abcdef"
	server := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err != nil {
			server <- err
			return
		}
		if request.Options.JoinCode != options.JoinCode ||
			request.Options.Controller != options.Controller ||
			request.Options.DiscoveryTimeout != options.DiscoveryTimeout ||
			request.Options.ConnectionTimeout != options.ConnectionTimeout {
			server <- errors.New("bootstrap request changed enrollment options")
			return
		}
		server <- request.Complete(nodeID, nil)
	}()
	receivedNodeID, err := submitBootstrapJoin(
		context.Background(),
		options,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if receivedNodeID != nodeID {
		t.Fatalf("node ID = %q", receivedNodeID)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapPipeReturnsFixedFailureWithoutLeakingCause(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	server := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err != nil {
			server <- err
			return
		}
		server <- request.Complete("", errors.New("secret upstream detail"))
	}()
	_, err := submitBootstrapJoin(context.Background(), options, false)
	if !errors.Is(err, ErrBootstrapEnrollment) ||
		strings.Contains(err.Error(), "secret upstream detail") {
		t.Fatalf("client error = %v", err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapProtocolRejectsDuplicateUnknownVersionAndOversize(t *testing.T) {
	valid := validBootstrapJoinOptions(t)
	frame := bootstrapRequestFrame{
		Version:                bootstrapProtocolV1,
		JoinCode:               valid.JoinCode,
		Controller:             valid.Controller,
		DiscoveryTimeoutNanos:  int64(valid.DiscoveryTimeout),
		ConnectionTimeoutNanos: int64(valid.ConnectionTimeout),
	}
	payload, err := encodeBootstrapJSON(frame)
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		[]byte(`{"version":1,"version":1}`),
		[]byte(`{"version":1,"unknown":true}`),
		append(payload, []byte(`{"trailing":true}`)...),
		make([]byte, maxBootstrapFrame+1),
	}
	for index, candidate := range cases {
		var decoded bootstrapRequestFrame
		if err := decodeBootstrapJSON(candidate, &decoded); err == nil {
			t.Fatalf("invalid protocol case %d was accepted", index)
		}
	}
	frame.Version = bootstrapProtocolV1 + 1
	if err := validateBootstrapRequestFrame(frame); err == nil {
		t.Fatal("unsupported bootstrap version was accepted")
	}
}

func TestBootstrapPipeRejectsRemoteClientsByConstruction(t *testing.T) {
	if bootstrapPipeMode&syswindows.PIPE_REJECT_REMOTE_CLIENTS == 0 {
		t.Fatal("bootstrap pipe does not reject remote clients")
	}
}

func TestSubmitBootstrapJoinTimesOutWithoutService(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	options.ConnectionTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := submitBootstrapJoin(ctx, options, false); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("missing service error = %v", err)
	}
}

func TestSubmitBootstrapJoinTimesOutWhenServiceDoesNotAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	options.ConnectionTimeout = 100 * time.Millisecond
	server := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err != nil {
			server <- err
			return
		}
		select {
		case <-request.Disconnected():
			server <- request.Complete(
				"0123456789abcdef0123456789abcdef",
				nil,
			)
		case <-time.After(5 * time.Second):
			server <- errors.New("client did not disconnect after its deadline")
		}
	}()
	if _, err := submitBootstrapJoin(
		context.Background(),
		options,
		false,
	); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("missing bootstrap acknowledgement error = %v", err)
	}
	select {
	case err := <-server:
		if !errors.Is(err, ErrBootstrapUnavailable) {
			t.Fatalf("server completion after disconnect = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe timed-out client")
	}
}

func TestBootstrapJoinCannotReplayAfterOnePipeInstance(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	const nodeID = "abcdef0123456789abcdef0123456789"
	server := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err == nil {
			err = request.Complete(nodeID, nil)
		}
		server <- err
	}()
	if _, err := submitBootstrapJoin(context.Background(), options, false); err != nil {
		t.Fatal(err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	replayContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	options.ConnectionTimeout = 50 * time.Millisecond
	if _, err := submitBootstrapJoin(replayContext, options, false); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("replay error = %v", err)
	}
}

func TestBootstrapRequestDetectsClientDisconnectBeforeAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	server := make(chan *BootstrapRequest, 1)
	serverErr := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err != nil {
			serverErr <- err
			return
		}
		server <- request
	}()
	payload, err := encodeBootstrapJSON(bootstrapRequestFrame{
		Version:                bootstrapProtocolV1,
		JoinCode:               options.JoinCode,
		Controller:             options.Controller,
		DiscoveryTimeoutNanos:  int64(options.DiscoveryTimeout),
		ConnectionTimeoutNanos: int64(options.ConnectionTimeout),
	})
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := connectBootstrapPipe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case request := <-server:
		select {
		case <-request.Disconnected():
		case <-time.After(5 * time.Second):
			t.Fatal("server did not observe client disconnect")
		}
		if err := request.Complete(
			"0123456789abcdef0123456789abcdef",
			nil,
		); !errors.Is(err, ErrBootstrapUnavailable) {
			t.Fatalf("disconnect completion error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive bootstrap request")
	}
}

func TestProductionBootstrapReceiverRejectsNonSystemIdentity(t *testing.T) {
	sid, err := currentProcessSID()
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(sid, "S-1-5-18") {
		t.Skip("test process is LocalSystem")
	}
	if _, err := ReceiveBootstrapRequest(context.Background()); !errors.Is(err, ErrBootstrapIdentity) {
		t.Fatalf("non-System receiver error = %v", err)
	}
}

func TestSubmitRejectsPipeServerThatIsNotLocalSystemService(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	server := make(chan error, 1)
	go func() {
		request, err := receiveBootstrapRequest(context.Background(), false)
		if err == nil {
			err = request.Complete(
				"0123456789abcdef0123456789abcdef",
				nil,
			)
		}
		server <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := SubmitBootstrapJoin(ctx, options); !errors.Is(err, ErrBootstrapIdentity) {
		t.Fatalf("unowned server error = %v", err)
	}
	select {
	case <-server:
	case <-time.After(5 * time.Second):
		t.Fatal("unowned test server did not close")
	}
}

func validBootstrapJoinOptions(t *testing.T) BootstrapJoinOptions {
	t.Helper()
	var fingerprint [sha256.Size]byte
	if _, err := rand.Read(fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	code, err := enroll.NewJoinCode(
		fingerprint,
		[]string{"controller.example.test:7443"},
		rand.Reader,
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := code.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return BootstrapJoinOptions{
		JoinCode:          encoded,
		Controller:        "https://controller.example.test:7443",
		DiscoveryTimeout:  time.Second,
		ConnectionTimeout: 2 * time.Second,
	}
}
