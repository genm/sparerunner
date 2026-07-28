//go:build !linux

package main

import (
	"path/filepath"
	"time"
)

const injectorRunParent = "/run/sparerunner-live-injectors"

type injectorFileEvidence struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	UID    uint32 `json:"uid"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
}

type injectorEvidence struct {
	Version            int                  `json:"version"`
	Status             string               `json:"status"`
	Source             injectorFileEvidence `json:"source"`
	Copy               injectorFileEvidence `json:"copy"`
	PreparedObservedAt string               `json:"preparedObservedAt"`
	ArmedObservedAt    string               `json:"armedObservedAt,omitempty"`
	DisarmedObservedAt string               `json:"disarmedObservedAt,omitempty"`
}

func prepareInjector(liveConfig, string) error { return errEvidenceInvalid }
func executeInjector(liveConfig, string) error { return errEvidenceInvalid }
func cleanupInjectorCopy(liveConfig) error     { return errEvidenceInvalid }

func validatePreparedInjector(evidence injectorEvidence) error {
	if evidence.Version != evidenceVersion ||
		(evidence.Status != "prepared" && evidence.Status != "armed" &&
			evidence.Status != "disarmed") ||
		evidence.Source.UID != 0 || evidence.Copy.UID != 0 ||
		evidence.Source.SHA256 != evidence.Copy.SHA256 ||
		evidence.Source.Device == 0 || evidence.Source.Inode == 0 ||
		evidence.Copy.Device == 0 || evidence.Copy.Inode == 0 ||
		evidence.Source.Size <= 0 || evidence.Copy.Size != evidence.Source.Size ||
		evidence.Copy.Mode != 0o500 ||
		!canonicalAbsolutePath(evidence.Source.Path) ||
		!canonicalAbsolutePath(evidence.Copy.Path) ||
		filepath.Dir(filepath.Dir(evidence.Copy.Path)) != injectorRunParent {
		return errEvidenceInvalid
	}
	return nil
}

func validateInjectorManifest(
	evidence injectorEvidence,
	startedAt time.Time,
	finishedAt time.Time,
) error {
	if validatePreparedInjector(evidence) != nil || evidence.Status != "disarmed" {
		return errEvidenceInvalid
	}
	preparedAt, err := time.Parse(time.RFC3339Nano, evidence.PreparedObservedAt)
	if err != nil || preparedAt.After(startedAt) {
		return errEvidenceInvalid
	}
	armedAt, err := time.Parse(time.RFC3339Nano, evidence.ArmedObservedAt)
	if err != nil || armedAt.Before(preparedAt) || armedAt.After(finishedAt) {
		return errEvidenceInvalid
	}
	disarmedAt, err := time.Parse(time.RFC3339Nano, evidence.DisarmedObservedAt)
	if err != nil || disarmedAt.Before(armedAt) || disarmedAt.After(finishedAt.Add(time.Minute)) {
		return errEvidenceInvalid
	}
	return nil
}
