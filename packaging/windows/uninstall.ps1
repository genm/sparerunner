#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
    [ValidateNotNullOrEmpty()]
    [string] $InstallRoot = (Join-Path $env:ProgramFiles "Tewake"),

    [ValidateNotNullOrEmpty()]
    [string] $DataRoot = (Join-Path $env:ProgramData "Tewake"),

    [switch] $PurgeData
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$AgentService = "TewakeAgent"
$RunnerService = "TewakeRunnerIdentity"

function Get-CanonicalScopedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [string] $AllowedParent
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A Tewake uninstall path must be absolute."
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
        throw "A Tewake uninstall path escaped its owning Windows directory."
    }
    return $Canonical
}

function Assert-NoReparsePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    $Current = [System.IO.Path]::GetFullPath($Path)
    while ($Current) {
        if (Test-Path -LiteralPath $Current) {
            $Item = Get-Item -LiteralPath $Current -Force
            if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "A Tewake uninstall path contains a reparse point."
            }
        }
        $Parent = [System.IO.Directory]::GetParent($Current)
        if ($null -eq $Parent) {
            break
        }
        $Current = $Parent.FullName
    }
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
        throw "Windows Service Control Manager rejected Tewake removal."
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
Assert-NoReparsePath -Path $InstallRoot
Assert-NoReparsePath -Path $DataRoot

if (-not $PSCmdlet.ShouldProcess("Tewake Windows services", "Stop and delete")) {
    return
}

Invoke-ServiceControlIfPresent -Name $AgentService -Operation "stop"
Invoke-ServiceControlIfPresent -Name $RunnerService -Operation "stop"
Invoke-ServiceControlIfPresent -Name $AgentService -Operation "delete"
Invoke-ServiceControlIfPresent -Name $RunnerService -Operation "delete"

if (Test-Path -LiteralPath $InstallRoot) {
    Remove-Item -LiteralPath $InstallRoot -Recurse -Force
}

if ($PurgeData) {
    if (-not $PSCmdlet.ShouldProcess(
        $DataRoot,
        "Permanently remove enrollment state, journal, cache, and quarantined workspaces"
    )) {
        throw "Tewake data purge was not confirmed."
    }
    if (Test-Path -LiteralPath $DataRoot) {
        Remove-Item -LiteralPath $DataRoot -Recurse -Force
    }
}
else {
    Write-Output "Tewake data was preserved at $DataRoot."
}
