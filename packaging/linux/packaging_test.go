//go:build linux

package linuxpackaging

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestSystemdUnitsKeepNetworkAgentOutsideRootBoundary(t *testing.T) {
	agent := readPackagingFile(t, "systemd/sparerunner-agent.service")
	supervisor := readPackagingFile(t, "systemd/sparerunner-supervisor.service")
	tmpfiles := readPackagingFile(t, "tmpfiles.d/sparerunner.conf")

	if err := validateAgentUnit(agent); err != nil {
		t.Fatalf("agent unit: %v", err)
	}
	if err := validateSupervisorUnit(supervisor, tmpfiles); err != nil {
		t.Fatalf("supervisor unit: %v", err)
	}
}

func TestAgentUnitPolicyRejectsRootOrCgroupPrivilege(t *testing.T) {
	agent := readPackagingFile(t, "systemd/sparerunner-agent.service")
	for _, unsafe := range []string{
		strings.Replace(agent, "User=sparerunner-agent", "User=root", 1),
		strings.Replace(agent, "CapabilityBoundingSet=\n", "CapabilityBoundingSet=CAP_KILL\n", 1),
		strings.Replace(agent, "KillMode=control-group\n", "KillMode=control-group\nDelegate=yes\n", 1),
	} {
		if err := validateAgentUnit(unsafe); err == nil {
			t.Fatalf("unsafe network Agent unit was accepted:\n%s", unsafe)
		}
	}
}

func TestSupervisorUnitKeepsDelegatedCgroupRootOwned(t *testing.T) {
	supervisor := readPackagingFile(t, "systemd/sparerunner-supervisor.service")
	tmpfiles := readPackagingFile(t, "tmpfiles.d/sparerunner.conf")
	type layout struct {
		unit     string
		tmpfiles string
	}
	for name, unsafe := range map[string]layout{
		"agent primary group": {
			unit:     strings.Replace(supervisor, "Group=root", "Group=sparerunner-agent", 1),
			tmpfiles: tmpfiles,
		},
		"missing socket directory": {
			unit: supervisor,
			tmpfiles: strings.Replace(
				tmpfiles,
				"d /run/sparerunner-supervisor 0750 root sparerunner-agent -\n",
				"",
				1,
			),
		},
		"wrong socket directory owner": {
			unit:     supervisor,
			tmpfiles: strings.Replace(tmpfiles, "0750 root sparerunner-agent", "0750 root root", 1),
		},
		"systemd managed socket directory": {
			unit: strings.Replace(
				supervisor,
				"[Service]\n",
				"[Service]\nRuntimeDirectory=sparerunner-supervisor\nRuntimeDirectoryMode=0750\n",
				1,
			),
			tmpfiles: tmpfiles,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSupervisorUnit(unsafe.unit, unsafe.tmpfiles); err == nil {
				t.Fatalf("unsafe Supervisor layout was accepted:\n%s\n%s", unsafe.unit, unsafe.tmpfiles)
			}
		})
	}
}

func TestSysusersAndTmpfilesSeparateAgentSupervisorAndSlotState(t *testing.T) {
	sysusers := readPackagingFile(t, "sysusers.d/sparerunner.conf")
	tmpfiles := readPackagingFile(t, "tmpfiles.d/sparerunner.conf")

	for _, required := range []string{
		`u sparerunner-agent - "SpareRunner node agent"`,
		`u sparerunner-runner-0 - "SpareRunner native runner slot 0"`,
	} {
		if strings.Count(sysusers, required) != 1 {
			t.Fatalf("sysusers entry %q count is not one", required)
		}
	}
	for _, required := range []string{
		"d /run/sparerunner-supervisor 0750 root sparerunner-agent -",
		"d /var/lib/sparerunner-agent 0700 sparerunner-agent sparerunner-agent -",
		"d /var/cache/sparerunner-agent 0700 sparerunner-agent sparerunner-agent -",
		"d /var/lib/sparerunner-supervisor/fences 0700 root root -",
		"d /var/lib/sparerunner-runtime 0711 root root -",
		"d /var/lib/sparerunner-runtime/executions 0711 sparerunner-agent sparerunner-agent -",
		"d /var/lib/sparerunner-runner/0 0700 sparerunner-runner-0 sparerunner-runner-0 -",
	} {
		if strings.Count(tmpfiles, required) != 1 {
			t.Fatalf("tmpfiles entry %q count is not one", required)
		}
	}
}

func validateAgentUnit(unit string) error {
	for _, required := range []string{
		"User=sparerunner-agent",
		"Group=sparerunner-agent",
		"Wants=sparerunner-supervisor.service",
		"CapabilityBoundingSet=\n",
		"ProtectControlGroups=yes",
		"--supervisor-socket=/run/sparerunner-supervisor/supervisor.sock",
		"--require-native-runner",
	} {
		if !strings.Contains(unit, required) {
			return errors.New("missing unprivileged Agent policy")
		}
	}
	for _, forbidden := range []string{
		"User=root",
		"Delegate=yes",
		"--runner-user=",
		"CapabilityBoundingSet=CAP_",
		"Environment=",
		"jitconfig",
	} {
		if strings.Contains(unit, forbidden) {
			return errors.New("network Agent retained a privileged or secret-bearing directive")
		}
	}
	return nil
}

func validateSupervisorUnit(unit, tmpfiles string) error {
	for _, required := range []string{
		"User=root",
		"Group=root",
		"After=systemd-tmpfiles-setup.service",
		"Delegate=yes",
		"--runner-user=sparerunner-runner-0",
		"--agent-user=sparerunner-agent",
		"--fence-root=/var/lib/sparerunner-supervisor/fences",
		"--cache-root=/var/cache/sparerunner-agent",
		"ProtectControlGroups=no",
		"TimeoutStopSec=30s",
	} {
		if !strings.Contains(unit, required) {
			return errors.New("missing privileged Supervisor policy")
		}
	}
	for _, directive := range []string{
		"User=root",
		"Group=root",
		"After=systemd-tmpfiles-setup.service",
	} {
		if countDirective(unit, directive) != 1 {
			return errors.New("Supervisor ownership directive is missing or ambiguous")
		}
	}
	if countDirective(tmpfiles, "d /run/sparerunner-supervisor 0750 root sparerunner-agent -") != 1 {
		return errors.New("Supervisor socket directory ownership is missing or ambiguous")
	}
	if strings.Contains(unit, "ReadWritePaths=/var/lib/sparerunner-supervisor /var/lib/sparerunner-runtime /var/lib/sparerunner-runner/0") {
		return errors.New("Supervisor retained writable access to the persistent runner home")
	}
	for _, forbidden := range []string{
		"Group=sparerunner-agent",
		"ExecStartPre=",
		"RuntimeDirectory=",
		"RuntimeDirectoryMode=",
		"PrivateNetwork=yes",
		"RestrictAddressFamilies=AF_UNIX",
		"Environment=",
		"jitconfig",
	} {
		if strings.Contains(unit, forbidden) {
			return errors.New("Supervisor policy would block the runner or persist a secret")
		}
	}
	return nil
}

func countDirective(unit, want string) int {
	count := 0
	for _, line := range strings.Split(unit, "\n") {
		if strings.TrimSpace(line) == want {
			count++
		}
	}
	return count
}

func readPackagingFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}
