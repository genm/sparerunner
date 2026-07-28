#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
    [ValidateNotNullOrEmpty()]
    [string] $InstallRoot = (Join-Path $env:ProgramFiles "SpareRunner"),

    [ValidateNotNullOrEmpty()]
    [string] $DataRoot = (Join-Path $env:ProgramData "SpareRunner"),

    [switch] $PurgeData
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "safe-tree.ps1")
. (Join-Path $PSScriptRoot "ownership.ps1")

$AgentService = "SpareRunnerAgent"
$RunnerService = "SpareRunnerRunnerIdentity"
$SystemSid = [System.Security.Principal.SecurityIdentifier]::new("S-1-5-18")

function Get-CanonicalScopedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [string] $AllowedParent
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A SpareRunner uninstall path must be absolute."
    }
    $Canonical = [System.IO.Path]::GetFullPath($Path).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    $Parent = [System.IO.Path]::GetFullPath($AllowedParent).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    if ($Canonical.Length -le $Parent.Length -or
        -not $Canonical.StartsWith(
            $Parent + [System.IO.Path]::DirectorySeparatorChar,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "A SpareRunner uninstall path escaped its owning Windows directory."
    }
    return $Canonical
}

function Invoke-ServiceControlIfPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [ValidateSet("stop", "delete")]
        [string] $Operation
    )

    $Service = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if (-not $Service) {
        return
    }
    if ($Operation -eq "stop" -and $Service.Status -eq "Stopped") {
        return
    }
    & (Join-Path $env:SystemRoot "System32\sc.exe") $Operation $Name | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Windows Service Control Manager rejected SpareRunner removal."
    }
    if ($Operation -eq "stop") {
        $Service.WaitForStatus(
            [System.ServiceProcess.ServiceControllerStatus]::Stopped,
            [TimeSpan]::FromSeconds(30)
        )
    }
}

$InstallRoot = Get-CanonicalScopedPath -Path $InstallRoot -AllowedParent $env:ProgramFiles
$DataRoot = Get-CanonicalScopedPath -Path $DataRoot -AllowedParent $env:ProgramData
$AgentPresent = $null -ne (
    Get-Service -Name $AgentService -ErrorAction SilentlyContinue
)
$RunnerPresent = $null -ne (
    Get-Service -Name $RunnerService -ErrorAction SilentlyContinue
)
$Authority = Get-SpareRunnerUninstallAuthority `
    -InstallRoot $InstallRoot `
    -DataRoot $DataRoot `
    -ServicesPresent ($AgentPresent -or $RunnerPresent) `
    -PurgeData:$PurgeData `
    -OwnerSid $SystemSid
$InstallRoot = $Authority.InstallRoot
$DataRoot = $Authority.DataRoot
Assert-SpareRunnerNoReparsePath -Path $InstallRoot
Assert-SpareRunnerNoReparsePath -Path $DataRoot
if (Test-Path -LiteralPath $InstallRoot) {
    Assert-SpareRunnerNoReparseTree -Root $InstallRoot
}
if ($PurgeData -and (Test-Path -LiteralPath $DataRoot)) {
    # Validate every descendant before changing SCM state. A nested junction
    # must not turn an uninstall request into a partial uninstall.
    Assert-SpareRunnerNoReparseTree -Root $DataRoot
}

$PrimaryTargets = [System.Collections.Generic.List[string]]::new()
$PrimaryActions = [System.Collections.Generic.List[string]]::new()
if ($AgentPresent) {
    $PrimaryTargets.Add("service $AgentService")
}
if ($RunnerPresent) {
    $PrimaryTargets.Add("service $RunnerService")
}
if ($AgentPresent -or $RunnerPresent) {
    $PrimaryActions.Add("stop and delete the listed services")
}
if ($Authority.InstallExists) {
    $PrimaryTargets.Add("verified install root $InstallRoot")
    $PrimaryActions.Add("remove the listed install root")
}
if ($PrimaryTargets.Count -gt 0 -and -not $PSCmdlet.ShouldProcess(
    ($PrimaryTargets -join ", "),
    ($PrimaryActions -join "; ")
)) {
    return
}
if ($PurgeData -and $Authority.DataExists -and -not $PSCmdlet.ShouldProcess(
    $DataRoot,
    "Permanently remove enrollment state, journal, cache, and quarantined workspaces"
)) {
    throw "SpareRunner data purge was not confirmed."
}

Invoke-ServiceControlIfPresent -Name $AgentService -Operation "stop"
Invoke-ServiceControlIfPresent -Name $RunnerService -Operation "stop"
Invoke-ServiceControlIfPresent -Name $AgentService -Operation "delete"
Invoke-ServiceControlIfPresent -Name $RunnerService -Operation "delete"

if (Test-Path -LiteralPath $InstallRoot) {
    Remove-SpareRunnerTreeNoReparse -Root $InstallRoot
}

if ($PurgeData) {
    if (Test-Path -LiteralPath $DataRoot) {
        Remove-SpareRunnerTreeNoReparse -Root $DataRoot
    }
}
else {
    Write-Output "SpareRunner data was preserved at $DataRoot."
}
