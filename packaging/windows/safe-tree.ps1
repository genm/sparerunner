Set-StrictMode -Version Latest

function Get-TewakeSafeTreeInventory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Root
    )

    if (-not [System.IO.Path]::IsPathRooted($Root)) {
        throw "A Tewake removal root must be absolute."
    }
    $Root = [System.IO.Path]::GetFullPath($Root).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    if ($null -eq [System.IO.Directory]::GetParent($Root)) {
        throw "A filesystem volume root cannot be a Tewake removal root."
    }
    if (-not [System.IO.Directory]::Exists($Root)) {
        return [pscustomobject]@{
            Files = [string[]] @()
            Directories = [string[]] @()
        }
    }

    $Files = [System.Collections.Generic.List[string]]::new()
    $Directories = [System.Collections.Generic.List[string]]::new()
    $Pending = [System.Collections.Generic.Stack[string]]::new()
    $Pending.Push($Root)
    while ($Pending.Count -gt 0) {
        $Directory = $Pending.Pop()
        $Attributes = [System.IO.File]::GetAttributes($Directory)
        if (($Attributes -band [System.IO.FileAttributes]::Directory) -eq 0 -or
            ($Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "A Tewake removal tree contains a reparse point or non-directory root."
        }
        $Directories.Add($Directory)
        foreach ($Entry in [System.IO.Directory]::EnumerateFileSystemEntries($Directory)) {
            $EntryPath = [System.IO.Path]::GetFullPath($Entry)
            $EntryAttributes = [System.IO.File]::GetAttributes($EntryPath)
            if (($EntryAttributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "A Tewake removal tree contains a reparse point."
            }
            if (($EntryAttributes -band [System.IO.FileAttributes]::Directory) -ne 0) {
                $Pending.Push($EntryPath)
            }
            else {
                $Files.Add($EntryPath)
            }
        }
    }
    return [pscustomobject]@{
        Files = [string[]] $Files.ToArray()
        Directories = [string[]] $Directories.ToArray()
    }
}

function Assert-TewakeNoReparseTree {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Root
    )

    [void] (Get-TewakeSafeTreeInventory -Root $Root)
}

function Remove-TewakeTreeNoReparse {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Root
    )

    $Inventory = Get-TewakeSafeTreeInventory -Root $Root
    foreach ($File in $Inventory.Files) {
        $Attributes = [System.IO.File]::GetAttributes($File)
        if (($Attributes -band [System.IO.FileAttributes]::Directory) -ne 0 -or
            ($Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "A Tewake removal file changed identity after preflight."
        }
        [System.IO.File]::Delete($File)
    }
    for ($Index = $Inventory.Directories.Count - 1; $Index -ge 0; $Index--) {
        $Directory = $Inventory.Directories[$Index]
        $Attributes = [System.IO.File]::GetAttributes($Directory)
        if (($Attributes -band [System.IO.FileAttributes]::Directory) -eq 0 -or
            ($Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "A Tewake removal directory changed identity after preflight."
        }
        # Never pass recursive=true. A raced-in entry makes this fail closed
        # instead of being followed or deleted.
        [System.IO.Directory]::Delete($Directory, $false)
    }
}
