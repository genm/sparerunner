Set-StrictMode -Version Latest

if ($null -eq (Get-Command "Set-TewakePrivateAcl" -ErrorAction SilentlyContinue) -or
    $null -eq (Get-Command "Remove-TewakeTreeNoReparse" -ErrorAction SilentlyContinue)) {
    throw "The Tewake ownership and safe-tree authorities must be loaded first."
}

$script:TewakeLiveEvidenceMarkerName = ".tewake-live-evidence.json"

function Get-TewakeLiveCanonicalPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A Windows live path must be absolute."
    }
    $Canonical = [System.IO.Path]::GetFullPath($Path).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    if ([string]::IsNullOrWhiteSpace($Canonical)) {
        throw "A Windows live path is invalid."
    }
    return $Canonical
}

function Test-TewakeLivePathOverlap {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Left,
        [Parameter(Mandatory = $true)]
        [string] $Right
    )

    $Left = Get-TewakeLiveCanonicalPath -Path $Left
    $Right = Get-TewakeLiveCanonicalPath -Path $Right
    if ([string]::Equals(
            $Left,
            $Right,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        return $true
    }
    $Separator = [string] [System.IO.Path]::DirectorySeparatorChar
    return $Left.StartsWith(
        $Right + $Separator,
        [System.StringComparison]::OrdinalIgnoreCase
    ) -or $Right.StartsWith(
        $Left + $Separator,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Assert-TewakeLiveEvidenceIsolation {
    param(
        [Parameter(Mandatory = $true)]
        [string] $EvidenceDirectory,
        [Parameter(Mandatory = $true)]
        [string[]] $ProtectedPaths
    )

    $EvidenceDirectory = Get-TewakeLiveCanonicalPath -Path $EvidenceDirectory
    if ($null -eq [System.IO.Directory]::GetParent($EvidenceDirectory)) {
        throw "A filesystem volume root cannot be a live-evidence directory."
    }
    foreach ($ProtectedPath in $ProtectedPaths) {
        if (Test-TewakeLivePathOverlap `
                -Left $EvidenceDirectory `
                -Right $ProtectedPath) {
            throw "The live-evidence directory overlaps a protected Tewake path."
        }
    }
    return $EvidenceDirectory
}

function ConvertTo-TewakeLiveEvidenceMarkerText {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Root,
        [Parameter(Mandatory = $true)]
        [string] $RepositoryRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedCommitSha,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedAgentSha256,
        [Parameter(Mandatory = $true)]
        [string] $InstalledAgent,
        [Parameter(Mandatory = $true)]
        [string] $AgentStateDirectory,
        [Parameter(Mandatory = $true)]
        [string] $CacheRoot,
        [Parameter(Mandatory = $true)]
        [string] $RuntimeRoot
    )

    if ($ExpectedCommitSha -cnotmatch "^[0-9a-f]{40}$" -or
        $ExpectedAgentSha256 -cnotmatch "^[0-9a-f]{64}$") {
        throw "The live-evidence binding digest is invalid."
    }
    $Payload = [ordered]@{
        version = 1
        role = "windows-live-evidence"
        root = (Get-TewakeLiveCanonicalPath -Path $Root)
        repositoryRoot = (Get-TewakeLiveCanonicalPath -Path $RepositoryRoot)
        expectedCommitSha = $ExpectedCommitSha
        expectedAgentSha256 = $ExpectedAgentSha256
        installedAgent = (Get-TewakeLiveCanonicalPath -Path $InstalledAgent)
        agentStateDirectory = (Get-TewakeLiveCanonicalPath -Path $AgentStateDirectory)
        cacheRoot = (Get-TewakeLiveCanonicalPath -Path $CacheRoot)
        runtimeRoot = (Get-TewakeLiveCanonicalPath -Path $RuntimeRoot)
    }
    return ($Payload | ConvertTo-Json -Compress) + [Environment]::NewLine
}

function Write-TewakeLiveEvidenceMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot,
        [Parameter(Mandatory = $true)]
        [string] $BoundRoot,
        [Parameter(Mandatory = $true)]
        [string] $RepositoryRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedCommitSha,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedAgentSha256,
        [Parameter(Mandatory = $true)]
        [string] $InstalledAgent,
        [Parameter(Mandatory = $true)]
        [string] $AgentStateDirectory,
        [Parameter(Mandatory = $true)]
        [string] $CacheRoot,
        [Parameter(Mandatory = $true)]
        [string] $RuntimeRoot,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $Marker = Join-Path $ActualRoot $script:TewakeLiveEvidenceMarkerName
    $Payload = ConvertTo-TewakeLiveEvidenceMarkerText `
        -Root $BoundRoot `
        -RepositoryRoot $RepositoryRoot `
        -ExpectedCommitSha $ExpectedCommitSha `
        -ExpectedAgentSha256 $ExpectedAgentSha256 `
        -InstalledAgent $InstalledAgent `
        -AgentStateDirectory $AgentStateDirectory `
        -CacheRoot $CacheRoot `
        -RuntimeRoot $RuntimeRoot
    $Stream = [System.IO.FileStream]::new(
        $Marker,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::None
    )
    try {
        $Bytes = [System.Text.UTF8Encoding]::new($false).GetBytes($Payload)
        $Stream.Write($Bytes, 0, $Bytes.Length)
        $Stream.Flush($true)
    }
    finally {
        $Stream.Dispose()
    }
    Set-TewakePrivateAcl -Path $Marker -OwnerSid $OwnerSid
}

function Assert-TewakeLiveEvidenceRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot,
        [Parameter(Mandatory = $true)]
        [string] $BoundRoot,
        [Parameter(Mandatory = $true)]
        [string] $RepositoryRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedCommitSha,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedAgentSha256,
        [Parameter(Mandatory = $true)]
        [string] $InstalledAgent,
        [Parameter(Mandatory = $true)]
        [string] $AgentStateDirectory,
        [Parameter(Mandatory = $true)]
        [string] $CacheRoot,
        [Parameter(Mandatory = $true)]
        [string] $RuntimeRoot,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $ActualRoot = Get-TewakeLiveCanonicalPath -Path $ActualRoot
    $BoundRoot = Get-TewakeLiveCanonicalPath -Path $BoundRoot
    Assert-TewakePrivateAcl -Path $ActualRoot -OwnerSid $OwnerSid -Directory
    $Marker = Join-Path $ActualRoot $script:TewakeLiveEvidenceMarkerName
    Assert-TewakePrivateAcl -Path $Marker -OwnerSid $OwnerSid
    $MarkerItem = Get-Item -LiteralPath $Marker -Force -ErrorAction Stop
    if ($MarkerItem.Length -le 0 -or $MarkerItem.Length -gt 16384) {
        throw "The live-evidence ownership marker has an invalid size."
    }
    $Raw = [System.IO.File]::ReadAllText($Marker)
    $Value = $Raw | ConvertFrom-Json
    $ActualProperties = @($Value.PSObject.Properties.Name | Sort-Object)
    $ExpectedProperties = @(
        "agentStateDirectory",
        "cacheRoot",
        "expectedAgentSha256",
        "expectedCommitSha",
        "installedAgent",
        "repositoryRoot",
        "role",
        "root",
        "runtimeRoot",
        "version"
    ) | Sort-Object
    if (($ActualProperties -join "`n") -cne ($ExpectedProperties -join "`n") -or
        $Value.version -ne 1 -or
        $Value.role -cne "windows-live-evidence") {
        throw "The live-evidence ownership marker is invalid."
    }
    $Expected = ConvertTo-TewakeLiveEvidenceMarkerText `
        -Root $BoundRoot `
        -RepositoryRoot $RepositoryRoot `
        -ExpectedCommitSha $ExpectedCommitSha `
        -ExpectedAgentSha256 $ExpectedAgentSha256 `
        -InstalledAgent $InstalledAgent `
        -AgentStateDirectory $AgentStateDirectory `
        -CacheRoot $CacheRoot `
        -RuntimeRoot $RuntimeRoot
    if ($Raw -cne $Expected) {
        throw "The live-evidence directory belongs to a different live configuration."
    }
}

function Assert-TewakeFreshLiveEvidenceContents {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot
    )

    $Entries = @(
        [System.IO.Directory]::EnumerateFileSystemEntries($ActualRoot)
    )
    $ExpectedMarker = [System.IO.Path]::GetFullPath(
        (Join-Path $ActualRoot $script:TewakeLiveEvidenceMarkerName)
    )
    if ($Entries.Count -ne 1 -or
        -not [string]::Equals(
            [System.IO.Path]::GetFullPath($Entries[0]),
            $ExpectedMarker,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "A newly published live-evidence root contains unowned content."
    }
}

function Initialize-TewakeLiveEvidenceRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $EvidenceDirectory,
        [Parameter(Mandatory = $true)]
        [string[]] $ProtectedPaths,
        [Parameter(Mandatory = $true)]
        [string] $RepositoryRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedCommitSha,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedAgentSha256,
        [Parameter(Mandatory = $true)]
        [string] $InstalledAgent,
        [Parameter(Mandatory = $true)]
        [string] $AgentStateDirectory,
        [Parameter(Mandatory = $true)]
        [string] $CacheRoot,
        [Parameter(Mandatory = $true)]
        [string] $RuntimeRoot,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    # Isolation is checked before any directory or ACL mutation.
    $EvidenceDirectory = Assert-TewakeLiveEvidenceIsolation `
        -EvidenceDirectory $EvidenceDirectory `
        -ProtectedPaths $ProtectedPaths
    $Arguments = @{
        BoundRoot = $EvidenceDirectory
        RepositoryRoot = $RepositoryRoot
        ExpectedCommitSha = $ExpectedCommitSha
        ExpectedAgentSha256 = $ExpectedAgentSha256
        InstalledAgent = $InstalledAgent
        AgentStateDirectory = $AgentStateDirectory
        CacheRoot = $CacheRoot
        RuntimeRoot = $RuntimeRoot
        OwnerSid = $OwnerSid
    }
    if (Test-Path -LiteralPath $EvidenceDirectory) {
        Assert-TewakeLiveEvidenceRoot `
            -ActualRoot $EvidenceDirectory `
            @Arguments
        return [pscustomobject]@{
            Root = $EvidenceDirectory
            Created = $false
        }
    }
    $Parent = [System.IO.Path]::GetDirectoryName($EvidenceDirectory)
    Assert-TewakeNoReparsePath -Path $Parent
    $ParentItem = Get-Item -LiteralPath $Parent -Force -ErrorAction Stop
    if (-not $ParentItem.PSIsContainer) {
        throw "The live-evidence parent is not an existing directory."
    }
    $Staging = Join-Path $Parent (
        ".tewake-live-evidence-{0}.staging" -f
            [Guid]::NewGuid().ToString("N")
    )
    if (Test-Path -LiteralPath $Staging) {
        throw "A live-evidence staging path already exists."
    }
    [void] [System.IO.Directory]::CreateDirectory($Staging)
    $Published = $false
    try {
        Set-TewakePrivateAcl -Path $Staging -OwnerSid $OwnerSid -Directory
        Write-TewakeLiveEvidenceMarker `
            -ActualRoot $Staging `
            @Arguments
        Assert-TewakeLiveEvidenceRoot `
            -ActualRoot $Staging `
            @Arguments
        Assert-TewakeFreshLiveEvidenceContents -ActualRoot $Staging
        [System.IO.Directory]::Move($Staging, $EvidenceDirectory)
        $Published = $true
        Assert-TewakeLiveEvidenceRoot `
            -ActualRoot $EvidenceDirectory `
            @Arguments
        Assert-TewakeFreshLiveEvidenceContents -ActualRoot $EvidenceDirectory
        return [pscustomobject]@{
            Root = $EvidenceDirectory
            Created = $true
        }
    }
    catch {
        $PublishFailure = $_
        if ($Published -and (Test-Path -LiteralPath $EvidenceDirectory)) {
            try {
                Assert-TewakeLiveEvidenceRoot `
                    -ActualRoot $EvidenceDirectory `
                    @Arguments
                Assert-TewakeFreshLiveEvidenceContents `
                    -ActualRoot $EvidenceDirectory
                Remove-TewakeTreeNoReparse -Root $EvidenceDirectory
            }
            catch {
                throw (
                    "Live-evidence publication failed and its verified " +
                    "rollback could not complete: $($_.Exception.Message)"
                )
            }
        }
        throw $PublishFailure
    }
    finally {
        if (Test-Path -LiteralPath $Staging) {
            Remove-TewakeTreeNoReparse -Root $Staging
        }
    }
}

if ($null -eq ("Tewake.Live.WindowsCommandLine" -as [type])) {
    Add-Type -TypeDefinition @"
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Tewake.Live {
    public static class WindowsCommandLine {
        [DllImport("shell32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern IntPtr CommandLineToArgvW(
            string commandLine,
            out int argumentCount
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr LocalFree(IntPtr memory);

        public static string[] Parse(string commandLine) {
            if (String.IsNullOrWhiteSpace(commandLine)) {
                throw new ArgumentException("The SCM command line is empty.");
            }
            int count;
            IntPtr arguments = CommandLineToArgvW(commandLine, out count);
            if (arguments == IntPtr.Zero) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            try {
                if (count <= 0) {
                    throw new ArgumentException(
                        "The SCM command line has no executable."
                    );
                }
                string[] parsed = new string[count];
                for (int index = 0; index < count; index++) {
                    IntPtr value = Marshal.ReadIntPtr(
                        arguments,
                        index * IntPtr.Size
                    );
                    parsed[index] = Marshal.PtrToStringUni(value);
                }
                return parsed;
            }
            finally {
                LocalFree(arguments);
            }
        }
    }
}
"@
}

function Assert-TewakeServiceCommandLine {
    param(
        [Parameter(Mandatory = $true)]
        [string] $CommandLine,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedExecutable,
        [Parameter(Mandatory = $true)]
        [string[]] $ExpectedArguments,
        [int[]] $PathArgumentIndexes = @()
    )

    $Parsed = [Tewake.Live.WindowsCommandLine]::Parse($CommandLine)
    if ($Parsed.Count -ne ($ExpectedArguments.Count + 1)) {
        throw "A Tewake SCM command has missing, duplicate, or unknown arguments."
    }
    $ObservedExecutable = Get-TewakeLiveCanonicalPath -Path $Parsed[0]
    $ExpectedExecutable = Get-TewakeLiveCanonicalPath -Path $ExpectedExecutable
    if (-not [string]::Equals(
            $ObservedExecutable,
            $ExpectedExecutable,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "A Tewake SCM command points at an unexpected executable."
    }
    $PathIndexes = @{}
    foreach ($Index in $PathArgumentIndexes) {
        if ($Index -lt 0 -or $Index -ge $ExpectedArguments.Count -or
            $PathIndexes.Contains($Index)) {
            throw "A Tewake SCM path-argument contract is invalid."
        }
        $PathIndexes[$Index] = $true
    }
    for ($Index = 0; $Index -lt $ExpectedArguments.Count; $Index++) {
        $Observed = [string] $Parsed[$Index + 1]
        $Expected = [string] $ExpectedArguments[$Index]
        if ($PathIndexes.Contains($Index)) {
            $Observed = Get-TewakeLiveCanonicalPath -Path $Observed
            $Expected = Get-TewakeLiveCanonicalPath -Path $Expected
            if (-not [string]::Equals(
                    $Observed,
                    $Expected,
                    [System.StringComparison]::OrdinalIgnoreCase
                )) {
                throw "A Tewake SCM command has an unexpected configured path."
            }
        }
        elseif ($Observed -cne $Expected) {
            throw "A Tewake SCM command has an unexpected role, service, or flag."
        }
    }
    return ,$Parsed
}
