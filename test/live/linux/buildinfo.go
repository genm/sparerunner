package main

import (
	"debug/buildinfo"
)

func (liveAuthorityProbe) goBuildVCS(path string) (string, bool, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", false, errEvidenceInvalid
	}
	var revision string
	var modifiedValue string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if revision != "" {
				return "", false, errEvidenceInvalid
			}
			revision = setting.Value
		case "vcs.modified":
			if modifiedValue != "" {
				return "", false, errEvidenceInvalid
			}
			modifiedValue = setting.Value
		}
	}
	if revision == "" || (modifiedValue != "true" && modifiedValue != "false") {
		return "", false, errEvidenceInvalid
	}
	return revision, modifiedValue == "true", nil
}
