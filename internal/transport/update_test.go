package transport

import (
	"errors"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

func TestExecutionUpdateRoundTripAndRejectsUnknownError(t *testing.T) {
	want := ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   "command-1",
		ExecutionID: "execution-1",
		State:       domain.ExecutionRunning,
	}
	payload, err := EncodeExecutionUpdate(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeExecutionUpdate(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("update = %#v, want %#v", got, want)
	}
	payload = []byte(`{"nodeId":"node-1","commandId":"command-1","executionId":"execution-1","state":"failed","replayed":false,"errorCode":"raw error text"}`)
	if _, err := DecodeExecutionUpdate(payload); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("unknown error code accepted: %v", err)
	}
	for _, inconsistent := range [][]byte{
		[]byte(`{"nodeId":"node-1","commandId":"command-1","executionId":"execution-1","state":"cleaning","replayed":false,"errorCode":"reconciliation_required"}`),
		[]byte(`{"nodeId":"node-1","commandId":"command-1","executionId":"execution-1","state":"cleanup_failed","replayed":false}`),
	} {
		if _, err := DecodeExecutionUpdate(inconsistent); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("inconsistent state/error accepted: %s, %v", inconsistent, err)
		}
	}
}
