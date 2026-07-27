//go:build windows

package windows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLiveEvidenceRootRejectsForeignAndOverlappingPathsWithoutMutation(
	t *testing.T,
) {
	base := filepath.Join(t.TempDir(), "live evidence contract")
	script := `
. $args[0]
. $args[1]
. $args[2]
$ErrorActionPreference = "Stop"
$base = $args[3]
[void] [System.IO.Directory]::CreateDirectory($base)
$owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$repo = Join-Path $base "repo"
$install = Join-Path $base "install"
$data = Join-Path $base "data"
$state = Join-Path $data "state"
$cache = Join-Path $data "cache"
$runtime = Join-Path $data "runtime"
foreach ($path in @($repo, $install, $state, $cache, $runtime)) {
    [void] [System.IO.Directory]::CreateDirectory($path)
}
$binding = @{
    ProtectedPaths = @($repo, $install, $data, $state, $cache, $runtime)
    RepositoryRoot = $repo
    ExpectedCommitSha = ("a" * 40)
    ExpectedAgentSha256 = ("b" * 64)
    InstalledAgent = (Join-Path $install "tewake-agent.exe")
    AgentStateDirectory = $state
    CacheRoot = $cache
    RuntimeRoot = $runtime
    OwnerSid = $owner
}

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
                        $hash = [Convert]::ToHexString(
                            $sha.ComputeHash([System.IO.File]::ReadAllBytes($_.FullName))
                        )
                    }
                    finally {
                        $sha.Dispose()
                    }
                    "f|$relative|$($_.Length)|$hash|$sddl"
                }
            }
    ) -join [Environment]::NewLine
}

$foreign = Join-Path $base "foreign evidence"
[void] [System.IO.Directory]::CreateDirectory((Join-Path $foreign "nested"))
[System.IO.File]::WriteAllText(
    (Join-Path $foreign "nested\canary.txt"),
    "foreign.example.test"
)
Set-TewakePrivateAcl -Path $foreign -OwnerSid $owner -Directory
$beforeSddl = Get-Sddl $foreign
$beforeTree = Get-TreeSnapshot $foreign
$rejected = $false
try {
    $foreignArguments = @{} + $binding
    $foreignArguments.EvidenceDirectory = $foreign
    [void] (Initialize-TewakeLiveEvidenceRoot @foreignArguments)
}
catch {
    $rejected = $true
}
if (-not $rejected -or
    (Get-Sddl $foreign) -cne $beforeSddl -or
    (Get-TreeSnapshot $foreign) -cne $beforeTree) {
    exit 20
}

$overlapCandidates = @(
    $repo,
    $base,
    (Join-Path $repo "evidence")
)
$overlapIndex = 0
foreach ($overlap in $overlapCandidates) {
    $overlapIndex++
    if (-not (Test-TewakeLivePathOverlap -Left $overlap -Right $repo)) {
        exit (20 + $overlapIndex)
    }
    $overlapArguments = @{} + $binding
    $overlapArguments.EvidenceDirectory = $overlap
    try {
        [void] (Initialize-TewakeLiveEvidenceRoot @overlapArguments)
        exit (23 + $overlapIndex)
    }
    catch {}
}
if (Test-Path -LiteralPath (Join-Path $repo "evidence")) {
    exit 27
}

$owned = Join-Path $base "owned evidence"
$ownedArguments = @{} + $binding
$ownedArguments.EvidenceDirectory = $owned
$created = Initialize-TewakeLiveEvidenceRoot @ownedArguments
if (-not $created.Created) {
    exit 28
}
$reused = Initialize-TewakeLiveEvidenceRoot @ownedArguments
if ($reused.Created) {
    exit 29
}
$ownedSddl = Get-Sddl $owned
$ownedTree = Get-TreeSnapshot $owned
$wrongBinding = @{} + $binding
$wrongBinding.ExpectedCommitSha = ("c" * 40)
try {
    $wrongBinding.EvidenceDirectory = $owned
    [void] (Initialize-TewakeLiveEvidenceRoot @wrongBinding)
    exit 30
}
catch {}
if ((Get-Sddl $owned) -cne $ownedSddl -or
    (Get-TreeSnapshot $owned) -cne $ownedTree) {
    exit 31
}
`
	runPowerShell(
		t,
		script,
		livePath(t, "..", "..", "..", "packaging", "windows", "safe-tree.ps1"),
		livePath(t, "..", "..", "..", "packaging", "windows", "ownership.ps1"),
		livePath(t, "live-support.ps1"),
		base,
	)
}

func TestSCMCommandLineParserRejectsWrongAndAmbiguousContracts(t *testing.T) {
	base := filepath.Join(t.TempDir(), "SCM command contract")
	script := `
. $args[0]
. $args[1]
. $args[2]
$ErrorActionPreference = "Stop"
$base = $args[3]
$exe = Join-Path $base "Program Files\Tewake\tewake-agent.exe"
$state = Join-Path $base "ProgramData\Tewake\agent-state"
$cache = Join-Path $base "ProgramData\Tewake\cache"
$runtime = Join-Path $base "ProgramData\Tewake\runtime"
$expected = @(
    "windows-service",
    "--role",
    "agent",
    "--service-name",
    "TewakeAgent",
    "--state-dir",
    $state,
    "--cache-root",
    $cache,
    "--runtime-root",
    $runtime,
    "--runner-identity-service",
    "TewakeRunnerIdentity",
    "--require-native-runner"
)
$valid = (
    '"' + $exe + '" windows-service --role agent ' +
    '--service-name TewakeAgent --state-dir "' + $state + '" ' +
    '--cache-root "' + $cache + '" --runtime-root "' + $runtime + '" ' +
    '--runner-identity-service TewakeRunnerIdentity --require-native-runner'
)
$assertion = @{
    CommandLine = $valid
    ExpectedExecutable = $exe
    ExpectedArguments = $expected
    PathArgumentIndexes = @(6, 8, 10)
}
[void] (Assert-TewakeServiceCommandLine @assertion)
$runnerExpected = @(
    "windows-service",
    "--role",
    "runner-identity",
    "--service-name",
    "TewakeRunnerIdentity"
)
$runnerValid = (
    '"' + $exe + '" windows-service --role runner-identity ' +
    '--service-name TewakeRunnerIdentity'
)
$runnerAssertion = @{
    CommandLine = $runnerValid
    ExpectedExecutable = $exe
    ExpectedArguments = $runnerExpected
}
[void] (Assert-TewakeServiceCommandLine @runnerAssertion)

$invalid = @(
    $valid + " --role agent",
    $valid.Replace("--require-native-runner", "--unknown"),
    $valid.Replace("--role agent", "--role runner-identity"),
    $valid.Replace("--service-name TewakeAgent", "--service-name OtherAgent"),
    $valid.Replace($state, (Join-Path $base "wrong-state")),
    $valid.Replace("--require-native-runner", "")
)
foreach ($candidate in $invalid) {
    try {
        $assertion.CommandLine = $candidate
        [void] (Assert-TewakeServiceCommandLine @assertion)
        exit 30
    }
    catch {}
}
try {
    $runnerAssertion.CommandLine = $runnerValid.Replace(
        "TewakeRunnerIdentity",
        "OtherRunnerIdentity"
    )
    [void] (Assert-TewakeServiceCommandLine @runnerAssertion)
    exit 31
}
catch {}
try {
    $assertion.CommandLine = $valid.Replace(
        $exe,
        (Join-Path $base "other.exe")
    )
    [void] (Assert-TewakeServiceCommandLine @assertion)
    exit 32
}
catch {}
`
	runPowerShell(
		t,
		script,
		livePath(t, "..", "..", "..", "packaging", "windows", "safe-tree.ps1"),
		livePath(t, "..", "..", "..", "packaging", "windows", "ownership.ps1"),
		livePath(t, "live-support.ps1"),
		base,
	)
}

func TestWindowsLivePowerShellParses(t *testing.T) {
	for _, name := range []string{"live-support.ps1", "run.ps1"} {
		command := powershellFileCommand(t, `$tokens=$null; $errors=$null; [void][System.Management.Automation.Language.Parser]::ParseFile($args[0],[ref]$tokens,[ref]$errors); if ($errors.Count -ne 0) { $errors | ForEach-Object { [Console]::Error.WriteLine($_.Message) }; exit 1 }`, livePath(t, name))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s parse: %v\n%s", name, err, output)
		}
	}
}

func runPowerShell(t *testing.T, script string, arguments ...string) {
	t.Helper()
	if output, err := powershellFileCommand(t, script, arguments...).CombinedOutput(); err != nil {
		t.Fatalf("PowerShell contract: %v\n%s", err, output)
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

func livePath(t *testing.T, parts ...string) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Windows live test path")
	}
	values := append([]string{filepath.Dir(current)}, parts...)
	return filepath.Clean(filepath.Join(values...))
}
