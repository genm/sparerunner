//go:build windows

package windows

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/genm/tewake/internal/enroll"
	syswindows "golang.org/x/sys/windows"
)

func TestBootstrapPipeCompletesOnlyAfterDurableEnrollmentAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	pipeName := bootstrapTestPipeName(t)
	const nodeID = "0123456789abcdef0123456789abcdef"
	server := make(chan error, 1)
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, pipeName, false)
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
	})
	receivedNodeID, err := submitBootstrapJoin(
		context.Background(),
		pipeName,
		options,
		false,
	)
	if err != nil {
		select {
		case serverErr := <-server:
			t.Fatalf("bootstrap submit error = %v; server error = %v", err, serverErr)
		case <-time.After(time.Second):
		}
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
	pipeName := bootstrapTestPipeName(t)
	server := make(chan error, 1)
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, pipeName, false)
		if err != nil {
			server <- err
			return
		}
		server <- request.Complete("", errors.New("secret upstream detail"))
	})
	_, err := submitBootstrapJoin(context.Background(), pipeName, options, false)
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
	if _, err := submitBootstrapJoin(
		ctx,
		bootstrapTestPipeName(t),
		options,
		false,
	); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("missing service error = %v", err)
	}
}

func TestSubmitBootstrapJoinTimesOutWhenServiceDoesNotAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	// The timeout has to cover pipe creation as well as the missing ack, so it
	// stays well above server start-up latency. A budget tight enough to expire
	// before the server goroutine creates its pipe would strand that goroutine
	// in ConnectNamedPipe and assert nothing about acknowledgement.
	options.ConnectionTimeout = time.Second
	pipeName := bootstrapTestPipeName(t)
	server := make(chan error, 1)
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, pipeName, false)
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
	})
	if _, err := submitBootstrapJoin(
		context.Background(),
		pipeName,
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
	pipeName := bootstrapTestPipeName(t)
	const nodeID = "abcdef0123456789abcdef0123456789"
	server := make(chan error, 1)
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, pipeName, false)
		if err == nil {
			err = request.Complete(nodeID, nil)
		}
		server <- err
	})
	if _, err := submitBootstrapJoin(
		context.Background(),
		pipeName,
		options,
		false,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	replayContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	options.ConnectionTimeout = 50 * time.Millisecond
	if _, err := submitBootstrapJoin(
		replayContext,
		pipeName,
		options,
		false,
	); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("replay error = %v", err)
	}
}

func TestBootstrapRequestDetectsClientDisconnectBeforeAck(t *testing.T) {
	options := validBootstrapJoinOptions(t)
	pipeName := bootstrapTestPipeName(t)
	server := make(chan *BootstrapRequest, 1)
	serverErr := make(chan error, 1)
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, pipeName, false)
		if err != nil {
			serverErr <- err
			return
		}
		server <- request
	})
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
	connectContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	file, _, err := connectBootstrapPipe(connectContext, pipeName)
	if err != nil {
		select {
		case receiveErr := <-serverErr:
			t.Fatalf("bootstrap server rejected the pipe: %v", receiveErr)
		default:
			t.Fatal(err)
		}
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
	// SubmitBootstrapJoin is the production entry point, so this is the one test
	// that has to serve the real BootstrapPipeName.
	startBootstrapServer(t, func(ctx context.Context) {
		request, err := receiveBootstrapRequest(ctx, BootstrapPipeName, false)
		if err == nil {
			err = request.Complete(
				"0123456789abcdef0123456789abcdef",
				nil,
			)
		}
		server <- err
	})
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

// startBootstrapServer owns the lifetime of one bootstrap pipe server goroutine.
// The bootstrap pipe is deliberately single-instance
// (FILE_FLAG_FIRST_PIPE_INSTANCE), so a goroutine that outlives its test would
// make every later test fail with ErrBootstrapUnavailable. Cancelling the
// context releases a server still blocked in ConnectNamedPipe, and the test does
// not return until the goroutine has actually finished.
func startBootstrapServer(t *testing.T, serve func(ctx context.Context)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("bootstrap pipe server goroutine did not finish")
		}
	})
}

// bootstrapTestPipeName gives each test its own pipe name. Isolation has to come
// from the name because single-instance creation and the pipe ACL are security
// properties of the production bootstrap pipe and must stay exactly as shipped.
func bootstrapTestPipeName(t *testing.T) string {
	t.Helper()
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf(`\\.\pipe\TewakeEnrollTest-%d-%x`, os.Getpid(), suffix)
}

func validBootstrapJoinOptions(t *testing.T) BootstrapJoinOptions {
	t.Helper()
	t.Setenv("TEWAKE_WINDOWS_DEBUG", "1")
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
