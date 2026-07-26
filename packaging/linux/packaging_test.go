package linuxpackaging

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestSystemdUnitsKeepNetworkAgentOutsideRootBoundary(t *testing.T) {
	agent := readPackagingFile(t, "systemd/tewake-agent.service")
	supervisor := readPackagingFile(t, "systemd/tewake-supervisor.service")

	if err := validateAgentUnit(agent); err != nil {
		t.Fatalf("agent unit: %v", err)
	}
	if err := validateSupervisorUnit(supervisor); err != nil {
		t.Fatalf("supervisor unit: %v", err)
	}
}

func TestAgentUnitPolicyRejectsRootOrCgroupPrivilege(t *testing.T) {
	agent := readPackagingFile(t, "systemd/tewake-agent.service")
	for _, unsafe := range []string{
		strings.Replace(agent, "User=tewake-agent", "User=root", 1),
		strings.Replace(agent, "CapabilityBoundingSet=\n", "CapabilityBoundingSet=CAP_KILL\n", 1),
		strings.Replace(agent, "KillMode=control-group\n", "KillMode=control-group\nDelegate=yes\n", 1),
	} {
		if err := validateAgentUnit(unsafe); err == nil {
			t.Fatalf("unsafe network Agent unit was accepted:\n%s", unsafe)
		}
	}
}

func TestSysusersAndTmpfilesSeparateAgentSupervisorAndSlotState(t *testing.T) {
	sysusers := readPackagingFile(t, "sysusers.d/tewake.conf")
	tmpfiles := readPackagingFile(t, "tmpfiles.d/tewake.conf")

	for _, required := range []string{
		`u tewake-agent - "Tewake node agent"`,
		`u tewake-runner-0 - "Tewake native runner slot 0"`,
	} {
		if strings.Count(sysusers, required) != 1 {
			t.Fatalf("sysusers entry %q count is not one", required)
		}
	}
	for _, required := range []string{
		"d /var/lib/tewake-agent 0700 tewake-agent tewake-agent -",
		"d /var/cache/tewake-agent 0700 tewake-agent tewake-agent -",
		"d /var/lib/tewake-supervisor/fences 0700 root root -",
		"d /var/lib/tewake-runtime 0711 root root -",
		"d /var/lib/tewake-runtime/executions 0711 tewake-agent tewake-agent -",
		"d /var/lib/tewake-runner/0 0700 tewake-runner-0 tewake-runner-0 -",
	} {
		if strings.Count(tmpfiles, required) != 1 {
			t.Fatalf("tmpfiles entry %q count is not one", required)
		}
	}
}

func validateAgentUnit(unit string) error {
	for _, required := range []string{
		"User=tewake-agent",
		"Group=tewake-agent",
		"Wants=tewake-supervisor.service",
		"CapabilityBoundingSet=\n",
		"ProtectControlGroups=yes",
		"--supervisor-socket=/run/tewake-supervisor/supervisor.sock",
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

func validateSupervisorUnit(unit string) error {
	for _, required := range []string{
		"User=root",
		"Group=tewake-agent",
		"Delegate=yes",
		"--runner-user=tewake-runner-0",
		"--agent-user=tewake-agent",
		"--fence-root=/var/lib/tewake-supervisor/fences",
		"--cache-root=/var/cache/tewake-agent",
		"ProtectControlGroups=no",
		"TimeoutStopSec=30s",
	} {
		if !strings.Contains(unit, required) {
			return errors.New("missing privileged Supervisor policy")
		}
	}
	if strings.Contains(unit, "ReadWritePaths=/var/lib/tewake-supervisor /var/lib/tewake-runtime /var/lib/tewake-runner/0") {
		return errors.New("Supervisor retained writable access to the persistent runner home")
	}
	for _, forbidden := range []string{
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

func readPackagingFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}
