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
    [string] $InstallRoot = (Join-Path $env:ProgramFiles "Tewake"),

    [ValidateNotNullOrEmpty()]
    [string] $DataRoot = (Join-Path $env:ProgramData "Tewake")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$AgentService = "TewakeAgent"
$RunnerService = "TewakeRunnerIdentity"
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
        throw "A Tewake install path must be absolute."
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
        throw "A Tewake install path escaped its owning Windows directory."
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
                throw "A Tewake path contains a reparse point."
            }
        }
        $Parent = [System.IO.Directory]::GetParent($Current)
        if ($null -eq $Parent) {
            break
        }
        $Current = $Parent.FullName
    }
}

function Assert-SourceBinary {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A source binary path must be absolute."
    }
    Assert-NoReparsePath -Path $Path
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
    Assert-NoReparsePath -Path $Path
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
        throw "Windows Service Control Manager rejected the Tewake configuration."
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
        throw "A Tewake install file already exists; use the future upgrade workflow."
    }
    $Staging = Join-Path (
        [System.IO.Path]::GetDirectoryName($Destination)
    ) (".{0}.{1}.staging" -f [System.IO.Path]::GetFileName($Destination), [Guid]::NewGuid().ToString("N"))
    Copy-Item -LiteralPath $Source -Destination $Staging
    try {
        Assert-NoReparsePath -Path $Staging
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

if ((Get-Service -Name $AgentService -ErrorAction SilentlyContinue) -or
    (Get-Service -Name $RunnerService -ErrorAction SilentlyContinue)) {
    throw "A Tewake Windows service already exists; installation will not overwrite it."
}

$InstalledAgent = Join-Path $InstallRoot "tewake-agent.exe"
$InstalledCli = Join-Path $InstallRoot "tewake.exe"
$StateRoot = Join-Path $DataRoot "agent-state"
$CacheRoot = Join-Path $DataRoot "cache"
$RuntimeRoot = Join-Path $DataRoot "runtime"
$CreatedRunnerService = $false
$CreatedAgentService = $false
$InstalledFiles = [System.Collections.Generic.List[string]]::new()

try {
    Set-ProtectedDirectory -Path $InstallRoot
    Install-FileNoClobber -Source $AgentBinary -Destination $InstalledAgent
    $InstalledFiles.Add($InstalledAgent)
    Install-FileNoClobber -Source $CliBinary -Destination $InstalledCli
    $InstalledFiles.Add($InstalledCli)

    $RunnerCommand = (Quote-ServiceArgument $InstalledAgent) +
        " windows-service --role runner-identity --service-name $RunnerService"
    Invoke-ServiceControl -Arguments @(
        "create", $RunnerService,
        "binPath=", $RunnerCommand,
        "start=", "auto",
        "obj=", "NT SERVICE\$RunnerService",
        "DisplayName=", "Tewake Runner Identity"
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
        "DisplayName=", "Tewake Agent"
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
    foreach ($File in $InstalledFiles) {
        if (Test-Path -LiteralPath $File) {
            Remove-Item -LiteralPath $File -Force
        }
    }
    throw $InstallFailure
}

Write-Output "Tewake services are installed and waiting for enrollment."
Write-Output "Run this elevated command once: $InstalledCli join twk_..."
