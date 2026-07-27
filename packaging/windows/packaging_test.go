package windows_test

import (
	"fmt"
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
		`Assert-TewakeNoReparsePath`,
		`New-TewakeOwnedRoot`,
		`Assert-TewakeOwnershipMarker`,
		`the first release does not support upgrades`,
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
	rejectExisting := strings.Index(
		installer,
		`the first release does not support upgrades`,
	)
	claimRoot := strings.Index(installer, `[void] (New-TewakeOwnedRoot`)
	if rejectExisting < 0 || claimRoot < 0 || rejectExisting > claimRoot {
		t.Fatal("installer can claim a root before rejecting existing roots")
	}
}

func TestUninstallerPreservesDataUnlessExplicitlyPurged(t *testing.T) {
	uninstaller := readPackagingFile(t, "uninstall.ps1")
	required := []string{
		`[switch] $PurgeData`,
		`if ($PurgeData)`,
		`$PSCmdlet.ShouldProcess`,
		`$PrimaryTargets`,
		`service $AgentService`,
		`service $RunnerService`,
		`verified install root $InstallRoot`,
		`$PrimaryActions`,
		`stop and delete the listed services`,
		`remove the listed install root`,
		`Get-TewakeUninstallAuthority`,
		`Assert-TewakeNoReparsePath -Path $DataRoot`,
		`Assert-TewakeNoReparseTree -Root $DataRoot`,
		`Remove-TewakeTreeNoReparse -Root $DataRoot`,
		`Tewake data was preserved`,
	}
	for _, fragment := range required {
		if !strings.Contains(uninstaller, fragment) {
			t.Errorf("uninstaller is missing %q", fragment)
		}
	}
	if strings.Contains(uninstaller, `Remove-Item`) &&
		strings.Contains(uninstaller, `-Recurse`) {
		t.Fatal("uninstaller uses recursive path deletion")
	}
	preflight := strings.Index(
		uninstaller,
		`Assert-TewakeNoReparseTree -Root $DataRoot`,
	)
	mutation := strings.Index(
		uninstaller,
		`Invoke-ServiceControlIfPresent -Name $AgentService -Operation "stop"`,
	)
	if preflight < 0 || mutation < 0 || preflight > mutation {
		t.Fatal("data-tree preflight does not precede SCM mutation")
	}
	authority := strings.Index(uninstaller, `Get-TewakeUninstallAuthority`)
	if authority < 0 || authority > mutation {
		t.Fatal("ownership authority validation does not precede SCM mutation")
	}
	primaryPrompt := strings.Index(uninstaller, `$PrimaryTargets.Count -gt 0`)
	purgePrompt := strings.Index(
		uninstaller,
		`$PurgeData -and $Authority.DataExists -and -not $PSCmdlet.ShouldProcess`,
	)
	if primaryPrompt < 0 || purgePrompt < 0 ||
		primaryPrompt > purgePrompt || purgePrompt > mutation {
		t.Fatal("uninstall and data-purge confirmations are not independent preflight gates")
	}
}

func TestPowerShellPackagingParsesOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("PowerShell parser is available on the Windows CI job")
	}
	for _, name := range []string{
		"install.ps1",
		"ownership.ps1",
		"safe-tree.ps1",
		"uninstall.ps1",
	} {
		path := packagingPath(t, name)
		command := powershellFileCommand(t, `$tokens=$null; $errors=$null; [void][System.Management.Automation.Language.Parser]::ParseFile($args[0],[ref]$tokens,[ref]$errors); if ($errors.Count -ne 0) { $errors | ForEach-Object { [Console]::Error.WriteLine($_.Message) }; exit 1 }`, path)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%s parse: %v\n%s", name, err, output)
		}
	}
}

func powershellFileCommand(t *testing.T, script string, arguments ...string) *exec.Cmd {
	t.Helper()
	scriptPath := filepath.Join(t.TempDir(), "tewake-test.ps1")
	prefix := "$args = @(0..([int]$env:TEWAKE_TEST_PS_ARG_COUNT - 1) | ForEach-Object { [Environment]::GetEnvironmentVariable(\"TEWAKE_TEST_PS_ARG_$_\") })\n"
	if err := os.WriteFile(scriptPath, []byte(prefix+script), 0o600); err != nil {
		t.Fatal(err)
	}
	commandArguments := []string{"-NoProfile", "-NonInteractive", "-File", scriptPath}
	commandArguments = append(commandArguments, arguments...)
	command := exec.Command("powershell.exe", commandArguments...)
	command.Env = append(os.Environ(), fmt.Sprintf("TEWAKE_TEST_PS_ARG_COUNT=%d", len(arguments)))
	for index, argument := range arguments {
		command.Env = append(command.Env, fmt.Sprintf("TEWAKE_TEST_PS_ARG_%d=%s", index, argument))
	}
	return command
}

func TestOwnershipMarkersRejectForeignCrossRoleCrossInstallAndTamperedRoots(
	t *testing.T,
) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows ACL and ownership marker semantics require Windows")
	}
	command := powershellFileCommand(t, `
. $args[0]
. $args[1]
$ErrorActionPreference = "Stop"
$base = $args[2]
$owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$mutations = 0
function Get-Sddl([string] $path) {
    $sections = (
        [System.Security.AccessControl.AccessControlSections]::Owner -bor
        [System.Security.AccessControl.AccessControlSections]::Group -bor
        [System.Security.AccessControl.AccessControlSections]::Access
    )
    $security = if ((Get-Item -LiteralPath $path).PSIsContainer) {
        [System.IO.Directory]::GetAccessControl($path)
    } else {
        [System.IO.File]::GetAccessControl($path)
    }
    return $security.GetSecurityDescriptorSddlForm(
        $sections
    )
}
function Get-TreeSnapshot([string] $root) {
    return @(
        Get-ChildItem -LiteralPath $root -Force -Recurse |
            Sort-Object FullName |
            ForEach-Object {
                $relative = $_.FullName.Substring($root.Length)
                $sections = (
                    [System.Security.AccessControl.AccessControlSections]::Owner -bor
                    [System.Security.AccessControl.AccessControlSections]::Group -bor
                    [System.Security.AccessControl.AccessControlSections]::Access
                )
                $security = if ($_.PSIsContainer) {
                    [System.IO.Directory]::GetAccessControl($_.FullName)
                } else {
                    [System.IO.File]::GetAccessControl($_.FullName)
                }
                $sddl = $security.GetSecurityDescriptorSddlForm(
                    $sections
                )
                if ($_.PSIsContainer) {
                    "d|$relative|$sddl"
                }
                else {
                    $sha = [System.Security.Cryptography.SHA256]::Create()
                    try {
                        $hash = ([BitConverter]::ToString(
                            $sha.ComputeHash([System.IO.File]::ReadAllBytes($_.FullName))
                        )).Replace("-", "")
                    }
                    finally {
                        $sha.Dispose()
                    }
                    "f|$relative|$($_.Length)|$hash|$sddl"
                }
            }
    ) -join [Environment]::NewLine
}

$validInstall = Join-Path $base "valid-install"
$validData = Join-Path $base "valid-data"
$validId = [Guid]::NewGuid().ToString("D")
[void] (New-TewakeOwnedRoot -Path $validInstall -Role install -InstallationId $validId -OwnerSid $owner)
[void] (New-TewakeOwnedRoot -Path $validData -Role data -InstallationId $validId -OwnerSid $owner)
$valid = Get-TewakeUninstallAuthority -InstallRoot $validInstall -DataRoot $validData -ServicesPresent $true -OwnerSid $owner
if ($valid.InstallationId -cne $validId) { exit 20 }

$foreign = Join-Path $base "foreign"
[void] [System.IO.Directory]::CreateDirectory($foreign)
$foreignCanary = Join-Path $foreign "foreign-canary.txt"
[System.IO.File]::WriteAllText($foreignCanary, "foreign.example.test")
$foreignSddl = Get-Sddl $foreign
$foreignTree = Get-TreeSnapshot $foreign
try {
    [void] (Get-TewakeUninstallAuthority -InstallRoot $foreign -DataRoot (Join-Path $base "absent-data") -ServicesPresent $false -OwnerSid $owner)
    $mutations++
}
catch {}
try {
    [void] (New-TewakeOwnedRoot -Path $foreign -Role install -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
    $mutations++
}
catch {}

$crossRole = Join-Path $base "cross-role"
[void] (New-TewakeOwnedRoot -Path $crossRole -Role install -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
$crossRoleCanary = Join-Path $crossRole "cross-role-canary.txt"
[System.IO.File]::WriteAllText($crossRoleCanary, "cross-role.example.test")
$crossRoleSddl = Get-Sddl $crossRole
$crossRoleTree = Get-TreeSnapshot $crossRole
try {
    [void] (Get-TewakeUninstallAuthority -InstallRoot (Join-Path $base "absent-install") -DataRoot $crossRole -ServicesPresent $false -PurgeData -OwnerSid $owner)
    $mutations++
}
catch {}

$crossInstall = Join-Path $base "cross-install"
$crossData = Join-Path $base "cross-data"
[void] (New-TewakeOwnedRoot -Path $crossInstall -Role install -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
[void] (New-TewakeOwnedRoot -Path $crossData -Role data -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
$crossInstallCanary = Join-Path $crossInstall "cross-install-canary.txt"
[System.IO.File]::WriteAllText($crossInstallCanary, "cross-install.example.test")
$crossInstallSddl = Get-Sddl $crossInstall
$crossInstallTree = Get-TreeSnapshot $crossInstall
$crossDataSddl = Get-Sddl $crossData
$crossDataTree = Get-TreeSnapshot $crossData
try {
    [void] (Get-TewakeUninstallAuthority -InstallRoot $crossInstall -DataRoot $crossData -ServicesPresent $true -OwnerSid $owner)
    $mutations++
}
catch {}

$tamperedInstall = Join-Path $base "tampered-install"
$tamperedData = Join-Path $base "tampered-data"
$tamperedId = [Guid]::NewGuid().ToString("D")
[void] (New-TewakeOwnedRoot -Path $tamperedInstall -Role install -InstallationId $tamperedId -OwnerSid $owner)
[void] (New-TewakeOwnedRoot -Path $tamperedData -Role data -InstallationId $tamperedId -OwnerSid $owner)
$tamperedCanary = Join-Path $tamperedInstall "tampered-canary.txt"
[System.IO.File]::WriteAllText($tamperedCanary, "tampered.example.test")
[System.IO.File]::AppendAllText((Join-Path $tamperedInstall ".tewake-ownership.json"), " ")
$tamperedInstallSddl = Get-Sddl $tamperedInstall
$tamperedInstallTree = Get-TreeSnapshot $tamperedInstall
$tamperedDataSddl = Get-Sddl $tamperedData
$tamperedDataTree = Get-TreeSnapshot $tamperedData
try {
    [void] (Get-TewakeUninstallAuthority -InstallRoot $tamperedInstall -DataRoot $tamperedData -ServicesPresent $true -OwnerSid $owner)
    $mutations++
}
catch {}

if ($mutations -ne 0) { exit 21 }
foreach ($canary in @($foreignCanary, $crossRoleCanary, $crossInstallCanary, $tamperedCanary)) {
    if (-not [System.IO.File]::Exists($canary)) { exit 22 }
}
if ((Get-Sddl $foreign) -cne $foreignSddl -or
    (Get-TreeSnapshot $foreign) -cne $foreignTree -or
    (Get-Sddl $crossRole) -cne $crossRoleSddl -or
    (Get-TreeSnapshot $crossRole) -cne $crossRoleTree -or
    (Get-Sddl $crossInstall) -cne $crossInstallSddl -or
    (Get-TreeSnapshot $crossInstall) -cne $crossInstallTree -or
    (Get-Sddl $crossData) -cne $crossDataSddl -or
    (Get-TreeSnapshot $crossData) -cne $crossDataTree -or
    (Get-Sddl $tamperedInstall) -cne $tamperedInstallSddl -or
    (Get-TreeSnapshot $tamperedInstall) -cne $tamperedInstallTree -or
    (Get-Sddl $tamperedData) -cne $tamperedDataSddl -or
    (Get-TreeSnapshot $tamperedData) -cne $tamperedDataTree) {
    exit 23
}
`,
		packagingPath(t, "safe-tree.ps1"),
		packagingPath(t, "ownership.ps1"),
		t.TempDir(),
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("ownership marker gates: %v\n%s", err, output)
	}
}

func TestOwnedRootPostPublishFailureRollsBackOnlyVerifiedPublication(
	t *testing.T,
) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows ACL and ownership marker semantics require Windows")
	}
	command := powershellFileCommand(t, `
. $args[0]
. $args[1]
$ErrorActionPreference = "Stop"
$base = $args[2]
$owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$script:originalAssert = ${function:Assert-TewakeOwnershipMarker}

$rollbackRoot = Join-Path $base "rollback-root"
$script:assertCalls = 0
function Assert-TewakeOwnershipMarker {
    param(
        [string] $ActualRoot,
        [string] $ExpectedBoundRoot,
        [string] $ExpectedRole,
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )
    $script:assertCalls++
    if ($script:assertCalls -eq 2) {
        throw "injected post-move validation failure"
    }
    return & $script:originalAssert @PSBoundParameters
}
try {
    [void] (New-TewakeOwnedRoot -Path $rollbackRoot -Role install -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
    exit 30
}
catch {}
if ($script:assertCalls -ne 3 -or
    (Test-Path -LiteralPath $rollbackRoot)) {
    exit 31
}
if (Get-ChildItem -LiteralPath $base -Force |
    Where-Object { $_.Name -like ".tewake-owned-*.staging" }) {
    exit 32
}

$changedRoot = Join-Path $base "changed-root"
$script:assertCalls = 0
function Assert-TewakeOwnershipMarker {
    param(
        [string] $ActualRoot,
        [string] $ExpectedBoundRoot,
        [string] $ExpectedRole,
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )
    $script:assertCalls++
    if ($script:assertCalls -eq 2) {
        [System.IO.File]::AppendAllText(
            (Join-Path $ActualRoot ".tewake-ownership.json"),
            " "
        )
        throw "injected changed publication"
    }
    return & $script:originalAssert @PSBoundParameters
}
try {
    [void] (New-TewakeOwnedRoot -Path $changedRoot -Role data -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
    exit 33
}
catch {}
if ($script:assertCalls -ne 3 -or
    -not (Test-Path -LiteralPath $changedRoot)) {
    exit 34
}

$foreignContentRoot = Join-Path $base "foreign-content-root"
$script:assertCalls = 0
function Assert-TewakeOwnershipMarker {
    param(
        [string] $ActualRoot,
        [string] $ExpectedBoundRoot,
        [string] $ExpectedRole,
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )
    $script:assertCalls++
    if ($script:assertCalls -eq 2) {
        [System.IO.File]::WriteAllText(
            (Join-Path $ActualRoot "foreign-canary.txt"),
            "foreign.example.test"
        )
        throw "injected foreign content"
    }
    return & $script:originalAssert @PSBoundParameters
}
try {
    [void] (New-TewakeOwnedRoot -Path $foreignContentRoot -Role install -InstallationId ([Guid]::NewGuid().ToString("D")) -OwnerSid $owner)
    exit 35
}
catch {}
if ($script:assertCalls -ne 3 -or
    -not [System.IO.File]::Exists(
        (Join-Path $foreignContentRoot "foreign-canary.txt")
    )) {
    exit 36
}
`,
		packagingPath(t, "safe-tree.ps1"),
		packagingPath(t, "ownership.ps1"),
		t.TempDir(),
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("owned-root publication rollback: %v\n%s", err, output)
	}
}

func TestSafeTreePurgeRejectsNestedJunctionWithoutDeletingExternalCanary(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows junction semantics require the Windows CI job")
	}
	root := filepath.Join(t.TempDir(), "data")
	external := filepath.Join(t.TempDir(), "external")
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(external, 0o700); err != nil {
		t.Fatal(err)
	}
	canary := filepath.Join(external, "must-survive.txt")
	if err := os.WriteFile(canary, []byte("external-canary.example.test"), 0o600); err != nil {
		t.Fatal(err)
	}
	junction := filepath.Join(root, "nested", "outside")
	output, err := exec.Command(
		"cmd.exe",
		"/d",
		"/c",
		"mklink",
		"/J",
		junction,
		external,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("create test junction: %v\n%s", err, output)
	}
	command := powershellFileCommand(t, `. $args[0]; try { Remove-TewakeTreeNoReparse -Root $args[1]; exit 10 } catch { if (Test-Path -LiteralPath $args[2]) { exit 0 }; exit 11 }`,
		packagingPath(t, "safe-tree.ps1"),
		root,
		canary,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("safe purge junction gate: %v\n%s", err, output)
	}
	if _, err := os.Stat(canary); err != nil {
		t.Fatalf("external canary was removed: %v", err)
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
