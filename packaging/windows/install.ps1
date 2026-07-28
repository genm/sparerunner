#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $AgentBinary,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $CliBinary,

    [ValidateNotNullOrEmpty()]
    [string] $InstallRoot = (Join-Path $env:ProgramFiles "SpareRunner"),

    [ValidateNotNullOrEmpty()]
    [string] $DataRoot = (Join-Path $env:ProgramData "SpareRunner")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "safe-tree.ps1")
. (Join-Path $PSScriptRoot "ownership.ps1")

$AgentService = "SpareRunnerAgent"
$RunnerService = "SpareRunnerRunnerIdentity"
$SystemSid = [System.Security.Principal.SecurityIdentifier]::new("S-1-5-18")
$AdministratorsSid = [System.Security.Principal.SecurityIdentifier]::new("S-1-5-32-544")

function Get-CanonicalScopedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [string] $AllowedParent
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A SpareRunner install path must be absolute."
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
        throw "A SpareRunner install path escaped its owning Windows directory."
    }
    return $Canonical
}

function Assert-SourceBinary {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A source binary path must be absolute."
    }
    Assert-SpareRunnerNoReparsePath -Path $Path
    $Item = Get-Item -LiteralPath $Path -Force
    if ($Item.PSIsContainer -or $Item.Length -le 0) {
        throw "A source binary is not a non-empty regular file."
    }
    return $Item.FullName
}

function New-ProtectedDirectoryAcl {
    param(
        [System.Security.Principal.SecurityIdentifier] $RunnerSid,
        [switch] $RunnerReadOnly
    )

    $Acl = [System.Security.AccessControl.DirectorySecurity]::new()
    $Acl.SetOwner($SystemSid)
    $Acl.SetAccessRuleProtection($true, $false)
    $Inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    $Propagation = [System.Security.AccessControl.PropagationFlags]::None
    foreach ($Sid in @($SystemSid, $AdministratorsSid)) {
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $Sid,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            $Inheritance,
            $Propagation,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        [void] $Acl.AddAccessRule($Rule)
    }
    if ($RunnerReadOnly) {
        if ($null -eq $RunnerSid) {
            throw "The runner service SID is unavailable."
        }
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $RunnerSid,
            [System.Security.AccessControl.FileSystemRights]::ReadAndExecute,
            $Inheritance,
            $Propagation,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        [void] $Acl.AddAccessRule($Rule)
    }
    return $Acl
}

function Set-ProtectedDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [System.Security.Principal.SecurityIdentifier] $RunnerSid,
        [switch] $RunnerReadOnly
    )

    [void] [System.IO.Directory]::CreateDirectory($Path)
    Assert-SpareRunnerNoReparsePath -Path $Path
    $Acl = New-ProtectedDirectoryAcl -RunnerSid $RunnerSid -RunnerReadOnly:$RunnerReadOnly
    Set-Acl -LiteralPath $Path -AclObject $Acl
}

function Set-ProtectedBinary {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [System.Security.Principal.SecurityIdentifier] $RunnerSid,
        [switch] $RunnerReadOnly
    )

    $Acl = [System.Security.AccessControl.FileSecurity]::new()
    $Acl.SetOwner($SystemSid)
    $Acl.SetAccessRuleProtection($true, $false)
    foreach ($Sid in @($SystemSid, $AdministratorsSid)) {
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $Sid,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        [void] $Acl.AddAccessRule($Rule)
    }
    if ($RunnerReadOnly) {
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $RunnerSid,
            [System.Security.AccessControl.FileSystemRights]::ReadAndExecute,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        [void] $Acl.AddAccessRule($Rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $Acl
}

function Quote-ServiceArgument {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Value
    )

    if ($Value.IndexOf('"') -ge 0 -or
        $Value.IndexOf("`r") -ge 0 -or
        $Value.IndexOf("`n") -ge 0) {
        throw "A Windows service argument contains unsupported characters."
    }
    return '"' + $Value + '"'
}

function Invoke-ServiceControl {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    & (Join-Path $env:SystemRoot "System32\sc.exe") @Arguments | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Windows Service Control Manager rejected the SpareRunner configuration."
    }
}

function Install-FileNoClobber {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Source,
        [Parameter(Mandatory = $true)]
        [string] $Destination
    )

    if (Test-Path -LiteralPath $Destination) {
        throw "A SpareRunner install file already exists; use the future upgrade workflow."
    }
    $Staging = Join-Path (
        [System.IO.Path]::GetDirectoryName($Destination)
    ) (".{0}.{1}.staging" -f [System.IO.Path]::GetFileName($Destination), [Guid]::NewGuid().ToString("N"))
    Copy-Item -LiteralPath $Source -Destination $Staging
    try {
        Assert-SpareRunnerNoReparsePath -Path $Staging
        Move-Item -LiteralPath $Staging -Destination $Destination
    }
    finally {
        if (Test-Path -LiteralPath $Staging) {
            Remove-Item -LiteralPath $Staging -Force
        }
    }
}

$InstallRoot = Get-CanonicalScopedPath -Path $InstallRoot -AllowedParent $env:ProgramFiles
$DataRoot = Get-CanonicalScopedPath -Path $DataRoot -AllowedParent $env:ProgramData
$AgentBinary = Assert-SourceBinary -Path $AgentBinary
$CliBinary = Assert-SourceBinary -Path $CliBinary

if ((Test-Path -LiteralPath $InstallRoot) -or
    (Test-Path -LiteralPath $DataRoot)) {
    throw "A SpareRunner root already exists; the first release does not support upgrades."
}
if ((Get-Service -Name $AgentService -ErrorAction SilentlyContinue) -or
    (Get-Service -Name $RunnerService -ErrorAction SilentlyContinue)) {
    throw "A SpareRunner Windows service already exists; installation will not overwrite it."
}

$InstalledAgent = Join-Path $InstallRoot "sparerunner-agent.exe"
$InstalledCli = Join-Path $InstallRoot "sparerunner.exe"
$StateRoot = Join-Path $DataRoot "agent-state"
$CacheRoot = Join-Path $DataRoot "cache"
$RuntimeRoot = Join-Path $DataRoot "runtime"
$CreatedRunnerService = $false
$CreatedAgentService = $false
$CreatedInstallRoot = $false
$CreatedDataRoot = $false
$InstallationId = [Guid]::NewGuid().ToString("D")

try {
    [void] (New-SpareRunnerOwnedRoot `
        -Path $InstallRoot `
        -Role "install" `
        -InstallationId $InstallationId `
        -OwnerSid $SystemSid)
    $CreatedInstallRoot = $true
    [void] (New-SpareRunnerOwnedRoot `
        -Path $DataRoot `
        -Role "data" `
        -InstallationId $InstallationId `
        -OwnerSid $SystemSid)
    $CreatedDataRoot = $true
    Set-ProtectedDirectory -Path $InstallRoot
    Install-FileNoClobber -Source $AgentBinary -Destination $InstalledAgent
    Install-FileNoClobber -Source $CliBinary -Destination $InstalledCli

    $RunnerCommand = (Quote-ServiceArgument $InstalledAgent) +
        " windows-service --role runner-identity --service-name $RunnerService"
    Invoke-ServiceControl -Arguments @(
        "create", $RunnerService,
        "binPath=", $RunnerCommand,
        "start=", "auto",
        "obj=", "NT SERVICE\$RunnerService",
        "DisplayName=", "SpareRunner Runner Identity"
    )
    $CreatedRunnerService = $true
    Invoke-ServiceControl -Arguments @("sidtype", $RunnerService, "unrestricted")
    Invoke-ServiceControl -Arguments @(
        "failure", $RunnerService,
        "reset=", "86400",
        "actions=", "restart/5000/restart/15000/restart/60000"
    )
    Invoke-ServiceControl -Arguments @("failureflag", $RunnerService, "1")

    $RunnerSid = ([System.Security.Principal.NTAccount]::new(
        "NT SERVICE",
        $RunnerService
    )).Translate([System.Security.Principal.SecurityIdentifier])
    Set-ProtectedDirectory -Path $InstallRoot -RunnerSid $RunnerSid -RunnerReadOnly
    Set-ProtectedBinary -Path $InstalledAgent -RunnerSid $RunnerSid -RunnerReadOnly
    Set-ProtectedBinary -Path $InstalledCli
    Set-ProtectedDirectory -Path $DataRoot -RunnerSid $RunnerSid -RunnerReadOnly
    Set-ProtectedDirectory -Path $StateRoot
    Set-ProtectedDirectory -Path $CacheRoot
    Set-ProtectedDirectory -Path $RuntimeRoot -RunnerSid $RunnerSid -RunnerReadOnly

    $AgentCommand = (Quote-ServiceArgument $InstalledAgent) +
        " windows-service --role agent" +
        " --service-name $AgentService" +
        " --state-dir " + (Quote-ServiceArgument $StateRoot) +
        " --cache-root " + (Quote-ServiceArgument $CacheRoot) +
        " --runtime-root " + (Quote-ServiceArgument $RuntimeRoot) +
        " --runner-identity-service $RunnerService" +
        " --require-native-runner"
    Invoke-ServiceControl -Arguments @(
        "create", $AgentService,
        "binPath=", $AgentCommand,
        "start=", "auto",
        "obj=", "LocalSystem",
        "depend=", $RunnerService,
        "DisplayName=", "SpareRunner Agent"
    )
    $CreatedAgentService = $true
    Invoke-ServiceControl -Arguments @(
        "failure", $AgentService,
        "reset=", "86400",
        "actions=", "restart/5000/restart/15000/restart/60000"
    )
    Invoke-ServiceControl -Arguments @("failureflag", $AgentService, "1")
    Invoke-ServiceControl -Arguments @("start", $RunnerService)
    Invoke-ServiceControl -Arguments @("start", $AgentService)
    [void] (Assert-SpareRunnerOwnershipMarker `
        -ActualRoot $InstallRoot `
        -ExpectedBoundRoot $InstallRoot `
        -ExpectedRole "install" `
        -OwnerSid $SystemSid)
    [void] (Assert-SpareRunnerOwnershipMarker `
        -ActualRoot $DataRoot `
        -ExpectedBoundRoot $DataRoot `
        -ExpectedRole "data" `
        -OwnerSid $SystemSid)
}
catch {
    $InstallFailure = $_
    if ($CreatedAgentService) {
        Stop-Service -Name $AgentService -Force -ErrorAction SilentlyContinue
        & (Join-Path $env:SystemRoot "System32\sc.exe") delete $AgentService | Out-Null
    }
    if ($CreatedRunnerService) {
        Stop-Service -Name $RunnerService -Force -ErrorAction SilentlyContinue
        & (Join-Path $env:SystemRoot "System32\sc.exe") delete $RunnerService | Out-Null
    }
    $CleanupFailures = [System.Collections.Generic.List[string]]::new()
    if ($CreatedDataRoot -and (Test-Path -LiteralPath $DataRoot)) {
        try {
            Remove-SpareRunnerTreeNoReparse -Root $DataRoot
        }
        catch {
            $CleanupFailures.Add("data root: $($_.Exception.Message)")
        }
    }
    if ($CreatedInstallRoot -and (Test-Path -LiteralPath $InstallRoot)) {
        try {
            Remove-SpareRunnerTreeNoReparse -Root $InstallRoot
        }
        catch {
            $CleanupFailures.Add("install root: $($_.Exception.Message)")
        }
    }
    if ($CleanupFailures.Count -gt 0) {
        throw "SpareRunner installation failed and owned-root cleanup also failed: $($CleanupFailures -join '; ')"
    }
    throw $InstallFailure
}

Write-Output "SpareRunner services are installed and waiting for enrollment."
Write-Output "Run this elevated command once: & `"$InstalledCli`" join spr_..."
