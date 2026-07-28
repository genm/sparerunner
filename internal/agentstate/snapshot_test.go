package agentstate

import (
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

func TestDigestCanonicalizesNilAndEmptyJournalCollections(t *testing.T) {
	base := Snapshot{
		NodeID:             "00000000000000000000000000000001",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      "2.336.0",
		MaxControllerEpoch: 1,
	}
	empty := base
	empty.Commands = []domain.Command{}
	empty.Observations = []Observation{}
	empty.CleanupTombstones = []CleanupTombstone{}

	nilDigest, err := Digest(base)
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, err := Digest(empty)
	if err != nil {
		t.Fatal(err)
	}
	if nilDigest != emptyDigest {
		t.Fatalf(
			"equivalent empty journals produced different digests: %s != %s",
			nilDigest,
			emptyDigest,
		)
	}
}
