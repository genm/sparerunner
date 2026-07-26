package windows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInstallerPinsServiceIdentityRecoveryAndFilesystemBoundary(t *testing.T) {
	installer := readPackagingFile(t, "install.ps1")
	required := []string{
		`#Requires -RunAsAdministrator`,
		`SetAccessRuleProtection($true, $false)`,
		`windows-service --role runner-identity`,
		`windows-service --role agent`,
		`obj=", "NT SERVICE\$RunnerService"`,
		`obj=", "LocalSystem"`,
		`"depend=", $RunnerService`,
		`"sidtype", $RunnerService, "unrestricted"`,
		`"failureflag", $RunnerService, "1"`,
		`"failureflag", $AgentService, "1"`,
		`--require-native-runner`,
		`Install-FileNoClobber`,
		`Assert-NoReparsePath`,
	}
	for _, fragment := range required {
		if !strings.Contains(installer, fragment) {
			t.Errorf("installer is missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`JoinCode`,
		`.env`,
		`cmd.exe`,
		`powershell.exe -Command`,
	} {
		if strings.Contains(installer, forbidden) {
			t.Errorf("installer contains forbidden credential/shell fragment %q", forbidden)
		}
	}
}

func TestUninstallerPreservesDataUnlessExplicitlyPurged(t *testing.T) {
	uninstaller := readPackagingFile(t, "uninstall.ps1")
	required := []string{
		`[switch] $PurgeData`,
		`if ($PurgeData)`,
		`$PSCmdlet.ShouldProcess`,
		`Assert-NoReparsePath -Path $DataRoot`,
		`Tewake data was preserved`,
	}
	for _, fragment := range required {
		if !strings.Contains(uninstaller, fragment) {
			t.Errorf("uninstaller is missing %q", fragment)
		}
	}
	if strings.Contains(uninstaller, `Remove-Item -LiteralPath $DataRoot -Recurse -Force`+"`n}") {
		t.Fatal("data removal is not visibly nested under the purge gate")
	}
}

func TestPowerShellPackagingParsesOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("PowerShell parser is available on the Windows CI job")
	}
	for _, name := range []string{"install.ps1", "uninstall.ps1"} {
		path := packagingPath(t, name)
		command := exec.Command(
			"powershell.exe",
			"-NoProfile",
			"-NonInteractive",
			"-Command",
			`$tokens=$null; $errors=$null; [void][System.Management.Automation.Language.Parser]::ParseFile($args[0],[ref]$tokens,[ref]$errors); if ($errors.Count -ne 0) { $errors | ForEach-Object { [Console]::Error.WriteLine($_.Message) }; exit 1 }`,
			path,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%s parse: %v\n%s", name, err, output)
		}
	}
}

func readPackagingFile(t *testing.T, name string) string {
	t.Helper()
	payload, err := os.ReadFile(packagingPath(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func packagingPath(t *testing.T, name string) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve packaging test path")
	}
	return filepath.Join(filepath.Dir(current), name)
}
