<#
.SYNOPSIS
    Build offline install bundle for Windows x86_64.

.DESCRIPTION
    Run on a host WITH internet access:
        git clone <repo>
        cd illumio_s3_collector
        .\scripts\build_offline_bundle.ps1
        -> dist\illumio-collector-windows-x86_64-v1.0.zip

    The bundle includes Python 3.11 runtime + all wheels.
    NSSM (Windows service manager) is included if nssm.cc is reachable;
    if the download fails, install.ps1 falls back to New-Service.
    The target (offline) host needs only Windows 10 / Server 2016+.
    No Python, pip, or internet required on the target.

    To update: git pull; .\scripts\build_offline_bundle.ps1
#>
param(
    [string]$Version = "1.0",
    [string]$PbsTag  = "20240415",
    [string]$PyVer   = "3.11.9"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$OutDir   = Join-Path $RepoRoot "dist"
$BuildDir = Join-Path $env:TEMP ("illumio-build-" + [guid]::NewGuid().ToString("N").Substring(0,8))
$Bundle   = Join-Path $BuildDir "bundle"

New-Item -ItemType Directory -Force -Path $OutDir, `
    (Join-Path $Bundle "app"), (Join-Path $Bundle "wheels") | Out-Null

Write-Host "==> Downloading python-build-standalone cpython-$PyVer+$PbsTag"
$PyUrl = "https://github.com/astral-sh/python-build-standalone/releases/download/$PbsTag/cpython-$PyVer+$PbsTag-x86_64-pc-windows-msvc-install_only.tar.gz"
Invoke-WebRequest -Uri $PyUrl -OutFile (Join-Path $Bundle "python-runtime.tar.gz")

Write-Host "==> Downloading wheels for win_amd64 / py3.11"
python -m pip download `
  --only-binary=:all: `
  --platform win_amd64 `
  --python-version 3.11 --implementation cp --abi cp311 `
  -d (Join-Path $Bundle "wheels") `
  -r (Join-Path $RepoRoot "requirements.txt")

Write-Host "==> Copying application code"
$AppDst = Join-Path $Bundle "app"
Copy-Item -Path (Join-Path $RepoRoot "collector.py"), `
              (Join-Path $RepoRoot "requirements.txt"), `
              (Join-Path $RepoRoot "config.example.yaml"), `
              (Join-Path $RepoRoot "README.md") -Destination $AppDst
foreach ($sub in "core","sources","mappers","sinks","mappings","fortisiem_parser","tests","docs") {
    Copy-Item -Recurse -Path (Join-Path $RepoRoot $sub) -Destination $AppDst
}

Write-Host "==> Downloading NSSM (optional — install.ps1 falls back to New-Service if missing)"
try {
    Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" `
        -OutFile (Join-Path $Bundle "nssm-2.24.zip") -ErrorAction Stop
    Write-Host "    NSSM downloaded OK"
} catch {
    Write-Warning "NSSM download failed ($_). Bundle will use New-Service fallback."
}

Write-Host "==> Copying install / uninstall / preflight scripts"
Copy-Item (Join-Path $RepoRoot "scripts/install.ps1")   $Bundle
Copy-Item (Join-Path $RepoRoot "scripts/uninstall.ps1") $Bundle
Copy-Item (Join-Path $RepoRoot "scripts/preflight.sh")  $Bundle

@"
illumio-s3-siem-collector v$Version
built: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ')
host:  $(hostname)
python: cpython-$PyVer+$PbsTag (x86_64 windows msvc)
"@ | Out-File (Join-Path $Bundle "VERSION") -Encoding UTF8

$Zip = Join-Path $OutDir "illumio-collector-windows-x86_64-v$Version.zip"
if (Test-Path $Zip) { Remove-Item $Zip }
Compress-Archive -Path (Join-Path $Bundle "*") -DestinationPath $Zip

$Hash = Get-FileHash $Zip -Algorithm SHA256
"$($Hash.Hash)  $(Split-Path $Zip -Leaf)" | Out-File (Join-Path $OutDir "SHA256SUMS-windows.txt") -Encoding ASCII

Write-Host "==> Done: $Zip"
Remove-Item -Recurse -Force $BuildDir
