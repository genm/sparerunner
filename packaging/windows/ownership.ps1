Set-StrictMode -Version Latest

if ($null -eq (Get-Command "Remove-TewakeTreeNoReparse" -ErrorAction SilentlyContinue)) {
    throw "The Tewake safe-tree authority must be loaded first."
}

$script:TewakeOwnershipMarkerName = ".tewake-ownership.json"

function Get-TewakeCanonicalRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not [System.IO.Path]::IsPathRooted($Path)) {
        throw "A Tewake owned root must be absolute."
    }
    $Canonical = [System.IO.Path]::GetFullPath($Path).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    if ($null -eq [System.IO.Directory]::GetParent($Canonical)) {
        throw "A filesystem volume root cannot be owned by Tewake."
    }
    return $Canonical
}

function Assert-TewakeNoReparsePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    $Current = [System.IO.Path]::GetFullPath($Path)
    while ($Current) {
        if (Test-Path -LiteralPath $Current) {
            $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
            if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "A Tewake owned path contains a reparse point."
            }
        }
        $Parent = [System.IO.Directory]::GetParent($Current)
        if ($null -eq $Parent) {
            break
        }
        $Current = $Parent.FullName
    }
}

function Get-TewakePrivateSids {
    param(
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $Required = [ordered]@{}
    foreach ($Sid in @(
        $OwnerSid,
        [System.Security.Principal.SecurityIdentifier]::new(
            [System.Security.Principal.WellKnownSidType]::LocalSystemSid,
            $null
        ),
        [System.Security.Principal.SecurityIdentifier]::new(
            [System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,
            $null
        )
    )) {
        $Required[$Sid.Value.ToUpperInvariant()] = $Sid
    }
    return ,$Required
}

function Get-TewakePathSecurity {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [switch] $Directory
    )
    if ($Directory) {
        return [System.IO.Directory]::GetAccessControl($Path)
    }
    return [System.IO.File]::GetAccessControl($Path)
}

function Set-TewakePathSecurity {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [System.Security.AccessControl.FileSystemSecurity] $Security,
        [switch] $Directory
    )
    if ($Directory) {
        [System.IO.Directory]::SetAccessControl($Path, [System.Security.AccessControl.DirectorySecurity] $Security)
        return
    }
    [System.IO.File]::SetAccessControl($Path, [System.Security.AccessControl.FileSecurity] $Security)
}

function Set-TewakePrivateAcl {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid,
        [switch] $Directory
    )

    Assert-TewakeNoReparsePath -Path $Path
    $Required = Get-TewakePrivateSids -OwnerSid $OwnerSid
    if ($Directory) {
        $Security = [System.Security.AccessControl.DirectorySecurity]::new()
        $Inheritance = (
            [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
            [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
        )
    }
    else {
        $Security = [System.Security.AccessControl.FileSecurity]::new()
        $Inheritance = [System.Security.AccessControl.InheritanceFlags]::None
    }
    $Security.SetOwner($OwnerSid)
    $Security.SetAccessRuleProtection($true, $false)
    foreach ($Sid in $Required.Values) {
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $Sid,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            $Inheritance,
            [System.Security.AccessControl.PropagationFlags]::None,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        [void] $Security.AddAccessRule($Rule)
    }
    Set-TewakePathSecurity -Path $Path -Security $Security -Directory:$Directory
}

function Assert-TewakePrivateAcl {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid,
        [switch] $Directory
    )

    Assert-TewakeNoReparsePath -Path $Path
    $Item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    if ([bool] $Directory -ne [bool] $Item.PSIsContainer -or
        ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "A Tewake private path has the wrong filesystem identity."
    }
    $Required = Get-TewakePrivateSids -OwnerSid $OwnerSid
    $Observed = Get-TewakePathSecurity -Path $Path -Directory:$Directory
    $ObservedOwner = $Observed.GetOwner(
        [System.Security.Principal.SecurityIdentifier]
    )
    $Rules = @($Observed.Access)
    if (-not $Observed.AreAccessRulesProtected -or
        $ObservedOwner.Value -cne $OwnerSid.Value -or
        $Rules.Count -ne $Required.Count) {
        throw "A Tewake private path ACL is not exact."
    }
    $ExpectedInheritance = [System.Security.AccessControl.InheritanceFlags]::None
    if ($Directory) {
        $ExpectedInheritance = (
            [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
            [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
        )
    }
    $Seen = @{}
    foreach ($Rule in $Rules) {
        $Sid = $Rule.IdentityReference.Translate(
            [System.Security.Principal.SecurityIdentifier]
        ).Value.ToUpperInvariant()
        if (-not $Required.Contains($Sid) -or
            $Seen.Contains($Sid) -or
            $Rule.IsInherited -or
            $Rule.AccessControlType -ne
                [System.Security.AccessControl.AccessControlType]::Allow -or
            $Rule.FileSystemRights -ne
                [System.Security.AccessControl.FileSystemRights]::FullControl -or
            $Rule.InheritanceFlags -ne $ExpectedInheritance -or
            $Rule.PropagationFlags -ne
                [System.Security.AccessControl.PropagationFlags]::None) {
            throw "A Tewake private path ACL contains an unexpected rule."
        }
        $Seen[$Sid] = $true
    }
    if ($Seen.Count -ne $Required.Count) {
        throw "A Tewake private path ACL is incomplete."
    }
}

function ConvertTo-TewakeOwnershipMarkerText {
    param(
        [Parameter(Mandatory = $true)]
        [string] $InstallationId,
        [Parameter(Mandatory = $true)]
        [ValidateSet("install", "data")]
        [string] $Role,
        [Parameter(Mandatory = $true)]
        [string] $BoundRoot
    )

    $Parsed = [Guid]::Empty
    if (-not [Guid]::TryParseExact(
        $InstallationId,
        "D",
        [ref] $Parsed
    ) -or $Parsed.ToString("D") -cne $InstallationId) {
        throw "A Tewake installation ID is not canonical."
    }
    $BoundRoot = Get-TewakeCanonicalRoot -Path $BoundRoot
    $Payload = [ordered]@{
        version = 1
        installationId = $InstallationId
        role = $Role
        root = $BoundRoot
    }
    return ($Payload | ConvertTo-Json -Compress) + [Environment]::NewLine
}

function Write-TewakeOwnershipMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot,
        [Parameter(Mandatory = $true)]
        [string] $BoundRoot,
        [Parameter(Mandatory = $true)]
        [string] $InstallationId,
        [Parameter(Mandatory = $true)]
        [ValidateSet("install", "data")]
        [string] $Role,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $Marker = Join-Path $ActualRoot $script:TewakeOwnershipMarkerName
    $Payload = ConvertTo-TewakeOwnershipMarkerText `
        -InstallationId $InstallationId `
        -Role $Role `
        -BoundRoot $BoundRoot
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

function Assert-TewakeOwnershipMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedBoundRoot,
        [Parameter(Mandatory = $true)]
        [ValidateSet("install", "data")]
        [string] $ExpectedRole,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $ActualRoot = Get-TewakeCanonicalRoot -Path $ActualRoot
    $ExpectedBoundRoot = Get-TewakeCanonicalRoot -Path $ExpectedBoundRoot
    Assert-TewakeNoReparsePath -Path $ActualRoot
    $RootItem = Get-Item -LiteralPath $ActualRoot -Force -ErrorAction Stop
    if (-not $RootItem.PSIsContainer) {
        throw "A Tewake owned root is not a directory."
    }
    $Marker = Join-Path $ActualRoot $script:TewakeOwnershipMarkerName
    Assert-TewakePrivateAcl -Path $Marker -OwnerSid $OwnerSid
    $MarkerItem = Get-Item -LiteralPath $Marker -Force -ErrorAction Stop
    if ($MarkerItem.Length -le 0 -or $MarkerItem.Length -gt 4096) {
        throw "A Tewake ownership marker has an invalid size."
    }
    $Raw = [System.IO.File]::ReadAllText($Marker)
    $Value = $Raw | ConvertFrom-Json
    $ActualProperties = @($Value.PSObject.Properties.Name | Sort-Object)
    $ExpectedProperties = @(
        "installationId",
        "role",
        "root",
        "version"
    ) | Sort-Object
    if (($ActualProperties -join "`n") -cne ($ExpectedProperties -join "`n") -or
        $Value.version -ne 1 -or
        $Value.role -cne $ExpectedRole -or
        -not [string]::Equals(
            [string] $Value.root,
            $ExpectedBoundRoot,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "A Tewake ownership marker is not bound to this root and role."
    }
    $Expected = ConvertTo-TewakeOwnershipMarkerText `
        -InstallationId ([string] $Value.installationId) `
        -Role ([string] $Value.role) `
        -BoundRoot ([string] $Value.root)
    if ($Raw -cne $Expected) {
        throw "A Tewake ownership marker is not canonical."
    }
    return [pscustomobject]@{
        InstallationId = [string] $Value.installationId
        Role = [string] $Value.role
        Root = $ExpectedBoundRoot
    }
}

function Assert-TewakeFreshOwnedRootPublication {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ActualRoot,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedBoundRoot,
        [Parameter(Mandatory = $true)]
        [ValidateSet("install", "data")]
        [string] $ExpectedRole,
        [Parameter(Mandatory = $true)]
        [string] $ExpectedInstallationId,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    Assert-TewakePrivateAcl `
        -Path $ActualRoot `
        -OwnerSid $OwnerSid `
        -Directory
    $Authority = Assert-TewakeOwnershipMarker `
        -ActualRoot $ActualRoot `
        -ExpectedBoundRoot $ExpectedBoundRoot `
        -ExpectedRole $ExpectedRole `
        -OwnerSid $OwnerSid
    $Entries = @(
        [System.IO.Directory]::EnumerateFileSystemEntries($ActualRoot)
    )
    $ExpectedMarker = [System.IO.Path]::GetFullPath(
        (Join-Path $ActualRoot $script:TewakeOwnershipMarkerName)
    )
    if ($Authority.InstallationId -cne $ExpectedInstallationId -or
        $Entries.Count -ne 1 -or
        -not [string]::Equals(
            [System.IO.Path]::GetFullPath($Entries[0]),
            $ExpectedMarker,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "A newly published Tewake root contains unowned content or identity."
    }
    return $Authority
}

function New-TewakeOwnedRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [ValidateSet("install", "data")]
        [string] $Role,
        [Parameter(Mandatory = $true)]
        [string] $InstallationId,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $Path = Get-TewakeCanonicalRoot -Path $Path
    if (Test-Path -LiteralPath $Path) {
        throw "A Tewake owned root already exists; upgrades are not supported."
    }
    $Parent = [System.IO.Path]::GetDirectoryName($Path)
    Assert-TewakeNoReparsePath -Path $Parent
    $Staging = Join-Path $Parent (
        ".tewake-owned-{0}.staging" -f [Guid]::NewGuid().ToString("N")
    )
    if (Test-Path -LiteralPath $Staging) {
        throw "A Tewake ownership staging path already exists."
    }
    [void] [System.IO.Directory]::CreateDirectory($Staging)
    $Published = $false
    try {
        Set-TewakePrivateAcl -Path $Staging -OwnerSid $OwnerSid -Directory
        Write-TewakeOwnershipMarker `
            -ActualRoot $Staging `
            -BoundRoot $Path `
            -InstallationId $InstallationId `
            -Role $Role `
            -OwnerSid $OwnerSid
        [void] (Assert-TewakeFreshOwnedRootPublication `
            -ActualRoot $Staging `
            -ExpectedBoundRoot $Path `
            -ExpectedRole $Role `
            -ExpectedInstallationId $InstallationId `
            -OwnerSid $OwnerSid)
        [System.IO.Directory]::Move($Staging, $Path)
        $Published = $true
        return Assert-TewakeFreshOwnedRootPublication `
            -ActualRoot $Path `
            -ExpectedBoundRoot $Path `
            -ExpectedRole $Role `
            -ExpectedInstallationId $InstallationId `
            -OwnerSid $OwnerSid
    }
    catch {
        $PublishFailure = $_
        if ($Published -and (Test-Path -LiteralPath $Path)) {
            try {
                # The caller cannot set its created-root flag until this
                # function returns. Roll back only after independently
                # re-establishing that the exact marker published by this call
                # still owns the destination.
                [void] (Assert-TewakeFreshOwnedRootPublication `
                    -ActualRoot $Path `
                    -ExpectedBoundRoot $Path `
                    -ExpectedRole $Role `
                    -ExpectedInstallationId $InstallationId `
                    -OwnerSid $OwnerSid)
                Remove-TewakeTreeNoReparse -Root $Path
            }
            catch {
                throw (
                    "Tewake root publication failed and the verified rollback " +
                    "could not complete: $($_.Exception.Message)"
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

function Get-TewakeUninstallAuthority {
    param(
        [Parameter(Mandatory = $true)]
        [string] $InstallRoot,
        [Parameter(Mandatory = $true)]
        [string] $DataRoot,
        [Parameter(Mandatory = $true)]
        [bool] $ServicesPresent,
        [switch] $PurgeData,
        [Parameter(Mandatory = $true)]
        [System.Security.Principal.SecurityIdentifier] $OwnerSid
    )

    $InstallRoot = Get-TewakeCanonicalRoot -Path $InstallRoot
    $DataRoot = Get-TewakeCanonicalRoot -Path $DataRoot
    $InstallExists = Test-Path -LiteralPath $InstallRoot
    $DataExists = Test-Path -LiteralPath $DataRoot
    $InstallAuthority = $null
    $DataAuthority = $null
    if ($InstallExists) {
        $InstallAuthority = Assert-TewakeOwnershipMarker `
            -ActualRoot $InstallRoot `
            -ExpectedBoundRoot $InstallRoot `
            -ExpectedRole "install" `
            -OwnerSid $OwnerSid
    }
    if ($DataExists) {
        $DataAuthority = Assert-TewakeOwnershipMarker `
            -ActualRoot $DataRoot `
            -ExpectedBoundRoot $DataRoot `
            -ExpectedRole "data" `
            -OwnerSid $OwnerSid
    }
    if ($ServicesPresent -and (-not $InstallExists -or -not $DataExists)) {
        throw "Tewake services exist without both owned roots."
    }
    if ($InstallExists -and $DataExists -and
        $InstallAuthority.InstallationId -cne $DataAuthority.InstallationId) {
        throw "Tewake owned roots belong to different installations."
    }
    $InstallationId = ""
    if ($null -ne $InstallAuthority) {
        $InstallationId = $InstallAuthority.InstallationId
    }
    elseif ($null -ne $DataAuthority) {
        $InstallationId = $DataAuthority.InstallationId
    }
    return [pscustomobject]@{
        InstallRoot = $InstallRoot
        DataRoot = $DataRoot
        InstallExists = [bool] $InstallExists
        DataExists = [bool] $DataExists
        PurgeData = [bool] $PurgeData
        InstallationId = $InstallationId
    }
}
