#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("unit", "service-preflight", "dpapi-identity", "service-recovery", "reboot-before", "reboot-after")]
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
    $Agent = Get-CimInstance Win32_Service -Filter "Name='SpareRunnerAgent'"
    $Runner = Get-CimInstance Win32_Service -Filter "Name='SpareRunnerRunnerIdentity'"
    if ($null -eq $Agent -or $null -eq $Runner) {
        throw "The packaged SpareRunner services are not installed."
    }
    $AgentArguments = @(
        "windows-service",
        "--role",
        "agent",
        "--service-name",
        "SpareRunnerAgent",
        "--state-dir",
        [string] $Settings.agentStateDirectory,
        "--cache-root",
        [string] $Settings.cacheRoot,
        "--runtime-root",
        [string] $Settings.runtimeRoot,
        "--runner-identity-service",
        "SpareRunnerRunnerIdentity",
        "--require-native-runner"
    )
    $RunnerArguments = @(
        "windows-service",
        "--role",
        "runner-identity",
        "--service-name",
        "SpareRunnerRunnerIdentity"
    )
    [void] (Assert-SpareRunnerServiceCommandLine `
        -CommandLine ([string] $Agent.PathName) `
        -ExpectedExecutable ([string] $Settings.installedAgent) `
        -ExpectedArguments $AgentArguments `
        -PathArgumentIndexes @(6, 8, 10))
    [void] (Assert-SpareRunnerServiceCommandLine `
        -CommandLine ([string] $Runner.PathName) `
        -ExpectedExecutable ([string] $Settings.installedAgent) `
        -ExpectedArguments $RunnerArguments)
    if ($Agent.Name -cne "SpareRunnerAgent" -or
        $Agent.State -cne "Running" -or
        $Agent.StartName -notin @("LocalSystem", "NT AUTHORITY\LocalSystem") -or
        $Agent.ProcessId -le 0) {
        throw "SpareRunnerAgent effective SCM state does not match the package contract."
    }
    if ($Runner.Name -cne "SpareRunnerRunnerIdentity" -or
        $Runner.State -cne "Running" -or
        $Runner.StartName -cne "NT SERVICE\SpareRunnerRunnerIdentity" -or
        $Runner.ProcessId -le 0 -or
        $Runner.ProcessId -eq $Agent.ProcessId) {
        throw "SpareRunnerRunnerIdentity effective SCM state does not match the package contract."
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
. (Join-Path $RepositoryRoot "packaging\windows\safe-tree.ps1")
. (Join-Path $RepositoryRoot "packaging\windows\ownership.ps1")
. (Join-Path $RepositoryRoot "test\live\windows\live-support.ps1")
$EvidenceOwner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
if ($null -eq $EvidenceOwner) {
    throw "The live-evidence owner SID is unavailable."
}
$AgentHash = (Get-FileHash -LiteralPath $Settings.installedAgent -Algorithm SHA256).Hash.ToLowerInvariant()
if ($AgentHash -cne $Settings.expectedAgentSha256) {
    throw "The installed Agent digest does not match the live config."
}
$InstallRoot = [System.IO.Path]::GetDirectoryName(
    [System.IO.Path]::GetFullPath($Settings.installedAgent)
)
$DataRoot = [System.IO.Path]::GetDirectoryName(
    [System.IO.Path]::GetFullPath($Settings.agentStateDirectory)
)
if (-not [string]::Equals(
        $DataRoot,
        [System.IO.Path]::GetDirectoryName(
            [System.IO.Path]::GetFullPath($Settings.cacheRoot)
        ),
        [System.StringComparison]::OrdinalIgnoreCase
    ) -or
    -not [string]::Equals(
        $DataRoot,
        [System.IO.Path]::GetDirectoryName(
            [System.IO.Path]::GetFullPath($Settings.runtimeRoot)
        ),
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
    throw "The live config paths do not share one data root."
}
$SystemSid = [System.Security.Principal.SecurityIdentifier]::new("S-1-5-18")
$InstallAuthority = Assert-SpareRunnerOwnershipMarker `
    -ActualRoot $InstallRoot `
    -ExpectedBoundRoot $InstallRoot `
    -ExpectedRole "install" `
    -OwnerSid $SystemSid
$DataAuthority = Assert-SpareRunnerOwnershipMarker `
    -ActualRoot $DataRoot `
    -ExpectedBoundRoot $DataRoot `
    -ExpectedRole "data" `
    -OwnerSid $SystemSid
if ($InstallAuthority.InstallationId -cne $DataAuthority.InstallationId) {
    throw "The installed roots have different ownership identities."
}
$EvidenceAuthority = Initialize-SpareRunnerLiveEvidenceRoot `
    -EvidenceDirectory ([string] $Settings.evidenceDirectory) `
    -ProtectedPaths @(
        $RepositoryRoot,
        $InstallRoot,
        $DataRoot,
        [string] $Settings.agentStateDirectory,
        [string] $Settings.cacheRoot,
        [string] $Settings.runtimeRoot
    ) `
    -RepositoryRoot $RepositoryRoot `
    -ExpectedCommitSha ([string] $Settings.expectedCommitSha) `
    -ExpectedAgentSha256 ([string] $Settings.expectedAgentSha256) `
    -InstalledAgent ([string] $Settings.installedAgent) `
    -AgentStateDirectory ([string] $Settings.agentStateDirectory) `
    -CacheRoot ([string] $Settings.cacheRoot) `
    -RuntimeRoot ([string] $Settings.runtimeRoot) `
    -OwnerSid $EvidenceOwner
$Settings.evidenceDirectory = $EvidenceAuthority.Root

switch ($Scenario) {
    "unit" {
        $Result = Join-Path $Settings.evidenceDirectory "windows-tests.jsonl"
        if (Test-Path -LiteralPath $Result) {
            throw "The Windows unit evidence already exists."
        }
        Push-Location $RepositoryRoot
        try {
            & mise exec -- go test -json `
                ./internal/app `
                ./internal/enroll `
                ./internal/platform/windows `
                ./internal/runner `
                ./cmd/sparerunner `
                ./cmd/sparerunner-agent `
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
        Assert-SpareRunnerPrivateAcl `
            -Path $Settings.agentStateDirectory `
            -OwnerSid $SystemSid `
            -Directory
        foreach ($Path in @(
            $Settings.cacheRoot,
            $Settings.runtimeRoot
        )) {
            $Acl = Get-Acl -LiteralPath $Path
            if (-not $Acl.AreAccessRulesProtected) {
                throw "A packaged SpareRunner directory has an inherited DACL."
            }
        }
        Write-Evidence -Name "service-preflight.json" -Value ([ordered]@{
            version = 1
            scenario = "service-preflight"
            commitSha = $Head
            agentSha256 = $AgentHash
            bootTime = [string] (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString("O")
            ownership = [ordered]@{
                installationId = [string] $InstallAuthority.InstallationId
                installRole = [string] $InstallAuthority.Role
                dataRole = [string] $DataAuthority.Role
            }
            services = $Services
        })
    }
    "dpapi-identity" {
        $Services = Get-ServiceEvidence
        $InteractiveSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        if ($null -eq $InteractiveSid -or
            $InteractiveSid.Value -ceq $SystemSid.Value) {
            throw "DPAPI cross-identity evidence must run outside LocalSystem."
        }
        $Locator = Join-Path $Settings.agentStateDirectory "node-private-key.pem"
        Assert-SpareRunnerPrivateAcl `
            -Path $Locator `
            -OwnerSid $SystemSid
        $Envelope = [System.IO.File]::ReadAllBytes($Locator)
        $Magic = [byte[]] @(
            [byte] [char] "T",
            [byte] [char] "W",
            [byte] [char] "K",
            [byte] [char] "D",
            [byte] [char] "P",
            [byte] [char] "A",
            [byte] [char] "P",
            [byte] [char] "I",
            1
        )
        if ($Envelope.Length -le $Magic.Length) {
            throw "The node-key locator is not a DPAPI envelope."
        }
        for ($Index = 0; $Index -lt $Magic.Length; $Index++) {
            if ($Envelope[$Index] -ne $Magic[$Index]) {
                throw "The node-key locator is not a DPAPI envelope."
            }
        }
        $Ciphertext = [byte[]]::new($Envelope.Length - $Magic.Length)
        [Array]::Copy(
            $Envelope,
            $Magic.Length,
            $Ciphertext,
            0,
            $Ciphertext.Length
        )
        $Entropy = [System.Text.Encoding]::UTF8.GetBytes(
            "sparerunner/private-material/windows/v1"
        )
        $Rejected = $false
        $UnexpectedPlaintext = $null
        try {
            $UnexpectedPlaintext = [System.Security.Cryptography.ProtectedData]::Unprotect(
                $Ciphertext,
                $Entropy,
                [System.Security.Cryptography.DataProtectionScope]::CurrentUser
            )
        }
        catch [System.Security.Cryptography.CryptographicException] {
            $Rejected = $true
        }
        finally {
            if ($null -ne $UnexpectedPlaintext) {
                [Array]::Clear(
                    $UnexpectedPlaintext,
                    0,
                    $UnexpectedPlaintext.Length
                )
            }
            [Array]::Clear($Ciphertext, 0, $Ciphertext.Length)
            [Array]::Clear($Envelope, 0, $Envelope.Length)
            [Array]::Clear($Entropy, 0, $Entropy.Length)
        }
        if (-not $Rejected) {
            throw "A non-LocalSystem identity decrypted Agent DPAPI material."
        }
        Write-Evidence -Name "dpapi-identity.json" -Value ([ordered]@{
            version = 1
            scenario = "dpapi-identity"
            interactiveSid = [string] $InteractiveSid.Value
            agentServiceStartName = [string] $Services.agent.startName
            locatorAclOwner = [string] $SystemSid.Value
            crossIdentityRejected = $true
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
        Restart-Service -Name "SpareRunnerAgent" -Force
        (Get-Service -Name "SpareRunnerAgent").WaitForStatus(
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
