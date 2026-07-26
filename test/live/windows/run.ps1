#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("unit", "service-preflight", "service-recovery", "reboot-before", "reboot-after")]
    [string] $Scenario,

    [Parameter(Mandatory = $true, Position = 1)]
    [ValidateNotNullOrEmpty()]
    [string] $Config,

    [switch] $ConfirmNoRunningJob
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-NoReparsePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A live-evidence path must be absolute."
    }
    $Current = [System.IO.Path]::GetFullPath($Path)
    while ($Current) {
        if (Test-Path -LiteralPath $Current) {
            $Item = Get-Item -LiteralPath $Current -Force
            if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "A live-evidence path contains a reparse point."
            }
        }
        $Parent = [System.IO.Directory]::GetParent($Current)
        if ($null -eq $Parent) {
            break
        }
        $Current = $Parent.FullName
    }
}

function Assert-ExactProperties {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Value,
        [Parameter(Mandatory = $true)]
        [string[]] $Expected
    )

    $Actual = @($Value.PSObject.Properties.Name | Sort-Object)
    $Wanted = @($Expected | Sort-Object)
    if (($Actual -join "`n") -cne ($Wanted -join "`n")) {
        throw "The Windows live config has missing or unknown fields."
    }
}

function Get-ServiceEvidence {
    $Agent = Get-CimInstance Win32_Service -Filter "Name='TewakeAgent'"
    $Runner = Get-CimInstance Win32_Service -Filter "Name='TewakeRunnerIdentity'"
    if ($null -eq $Agent -or $null -eq $Runner) {
        throw "The packaged Tewake services are not installed."
    }
    if ($Agent.State -cne "Running" -or
        $Agent.StartName -notin @("LocalSystem", "NT AUTHORITY\LocalSystem") -or
        $Agent.ProcessId -le 0 -or
        $Agent.PathName -notmatch "windows-service --role agent" -or
        $Agent.PathName -notmatch "--require-native-runner") {
        throw "TewakeAgent effective SCM state does not match the package contract."
    }
    if ($Runner.State -cne "Running" -or
        $Runner.StartName -cne "NT SERVICE\TewakeRunnerIdentity" -or
        $Runner.ProcessId -le 0 -or
        $Runner.ProcessId -eq $Agent.ProcessId -or
        $Runner.PathName -notmatch "windows-service --role runner-identity") {
        throw "TewakeRunnerIdentity effective SCM state does not match the package contract."
    }
    return [ordered]@{
        agent = [ordered]@{
            name = [string] $Agent.Name
            state = [string] $Agent.State
            startName = [string] $Agent.StartName
            processId = [uint32] $Agent.ProcessId
        }
        runnerIdentity = [ordered]@{
            name = [string] $Runner.Name
            state = [string] $Runner.State
            startName = [string] $Runner.StartName
            processId = [uint32] $Runner.ProcessId
        }
    }
}

function Write-Evidence {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [object] $Value
    )

    $Destination = Join-Path $Settings.evidenceDirectory $Name
    if (Test-Path -LiteralPath $Destination) {
        throw "The live-evidence file already exists; use a fresh evidence directory."
    }
    $Staging = $Destination + "." + [Guid]::NewGuid().ToString("N") + ".staging"
    $Payload = $Value | ConvertTo-Json -Depth 8 -Compress
    [System.IO.File]::WriteAllText(
        $Staging,
        $Payload + [Environment]::NewLine,
        [System.Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $Staging -Destination $Destination
}

$Config = [System.IO.Path]::GetFullPath($Config)
Assert-NoReparsePath -Path $Config
$RawConfig = [System.IO.File]::ReadAllText($Config)
$Settings = $RawConfig | ConvertFrom-Json
Assert-ExactProperties -Value $Settings -Expected @(
    "version",
    "evidenceDirectory",
    "expectedCommitSha",
    "expectedAgentSha256",
    "installedAgent",
    "agentStateDirectory",
    "cacheRoot",
    "runtimeRoot"
)
if ($Settings.version -ne 1 -or
    $Settings.expectedCommitSha -cnotmatch "^[0-9a-f]{40}$" -or
    $Settings.expectedAgentSha256 -cnotmatch "^[0-9a-f]{64}$") {
    throw "The Windows live config version or digest is invalid."
}
foreach ($Path in @(
    $Settings.evidenceDirectory,
    $Settings.installedAgent,
    $Settings.agentStateDirectory,
    $Settings.cacheRoot,
    $Settings.runtimeRoot
)) {
    Assert-NoReparsePath -Path $Path
}
[void] [System.IO.Directory]::CreateDirectory($Settings.evidenceDirectory)
$EvidenceAcl = Get-Acl -LiteralPath $Settings.evidenceDirectory
$EvidenceAcl.SetAccessRuleProtection($true, $false)
Set-Acl -LiteralPath $Settings.evidenceDirectory -AclObject $EvidenceAcl

$RepositoryRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot "..\..\..")
)
$Head = (& git -C $RepositoryRoot rev-parse --verify HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $Head -cne $Settings.expectedCommitSha) {
    throw "The live checkout commit does not match expectedCommitSha."
}
$Dirty = & git -C $RepositoryRoot status --porcelain=v1 --untracked-files=all
if ($LASTEXITCODE -ne 0 -or $Dirty) {
    throw "The live checkout is not clean."
}
$AgentHash = (Get-FileHash -LiteralPath $Settings.installedAgent -Algorithm SHA256).Hash.ToLowerInvariant()
if ($AgentHash -cne $Settings.expectedAgentSha256) {
    throw "The installed Agent digest does not match the live config."
}

switch ($Scenario) {
    "unit" {
        $Result = Join-Path $Settings.evidenceDirectory "windows-tests.jsonl"
        if (Test-Path -LiteralPath $Result) {
            throw "The Windows unit evidence already exists."
        }
        Push-Location $RepositoryRoot
        try {
            & mise exec -- go test -json `
                ./internal/platform/windows `
                ./cmd/tewake `
                ./cmd/tewake-agent `
                ./packaging/windows 2>&1 |
                Tee-Object -FilePath $Result
            if ($LASTEXITCODE -ne 0) {
                throw "The Windows platform test suite failed."
            }
        }
        finally {
            Pop-Location
        }
    }
    "service-preflight" {
        $Services = Get-ServiceEvidence
        foreach ($Path in @(
            $Settings.agentStateDirectory,
            $Settings.cacheRoot,
            $Settings.runtimeRoot
        )) {
            $Acl = Get-Acl -LiteralPath $Path
            if (-not $Acl.AreAccessRulesProtected) {
                throw "A packaged Tewake directory has an inherited DACL."
            }
        }
        Write-Evidence -Name "service-preflight.json" -Value ([ordered]@{
            version = 1
            scenario = "service-preflight"
            commitSha = $Head
            agentSha256 = $AgentHash
            bootTime = [string] (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString("O")
            services = $Services
        })
    }
    "service-recovery" {
        if (-not $ConfirmNoRunningJob) {
            throw "Service recovery closes Job Objects; explicitly confirm no job is running."
        }
        if (Get-Process -Name "Runner.Listener" -ErrorAction SilentlyContinue) {
            throw "A Runner.Listener process is active; recovery test refused."
        }
        $Before = Get-ServiceEvidence
        Restart-Service -Name "TewakeAgent" -Force
        (Get-Service -Name "TewakeAgent").WaitForStatus(
            [System.ServiceProcess.ServiceControllerStatus]::Running,
            [TimeSpan]::FromSeconds(30)
        )
        $After = Get-ServiceEvidence
        if ($Before.agent.processId -eq $After.agent.processId -or
            $Before.runnerIdentity.processId -ne $After.runnerIdentity.processId) {
            throw "Controlled Agent recovery changed the wrong process boundary."
        }
        Write-Evidence -Name "service-recovery.json" -Value ([ordered]@{
            version = 1
            scenario = "service-recovery"
            before = $Before
            after = $After
        })
    }
    "reboot-before" {
        $Services = Get-ServiceEvidence
        Write-Evidence -Name "reboot-before.json" -Value ([ordered]@{
            version = 1
            scenario = "reboot-before"
            bootTime = [string] (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString("O")
            services = $Services
        })
    }
    "reboot-after" {
        $BeforePath = Join-Path $Settings.evidenceDirectory "reboot-before.json"
        if (-not (Test-Path -LiteralPath $BeforePath)) {
            throw "reboot-before.json is missing."
        }
        Assert-NoReparsePath -Path $BeforePath
        $Before = [System.IO.File]::ReadAllText($BeforePath) | ConvertFrom-Json
        $BootTime = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString("O")
        if ($Before.bootTime -ceq $BootTime) {
            throw "The host has not rebooted since reboot-before evidence."
        }
        Write-Evidence -Name "reboot-after.json" -Value ([ordered]@{
            version = 1
            scenario = "reboot-after"
            priorBootTime = [string] $Before.bootTime
            bootTime = [string] $BootTime
            services = Get-ServiceEvidence
        })
    }
}
