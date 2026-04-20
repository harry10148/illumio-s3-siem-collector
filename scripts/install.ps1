<#
.SYNOPSIS
    Install the Illumio S3 -> SIEM Collector on Windows.

.DESCRIPTION
    Supports two modes:

    Bundle mode   — run from inside an extracted offline bundle
                    (python-runtime.tar.gz present alongside this script)
                    Administrator PowerShell inside bundle dir:
                        .\install.ps1

    Git-clone mode — run from the repository's scripts\ directory
                    (requires Python 3.x in PATH; internet access for pip)
                    Administrator PowerShell at repo root:
                        .\scripts\install.ps1
#>
param(
    [string]$InstallDir = "C:\illumio-collector"
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Error "This script must be run as Administrator."
    exit 1
}

$ScriptDir = $PSScriptRoot

# ---------- detect mode ----------
if (Test-Path (Join-Path $ScriptDir "python-runtime.tar.gz")) {
    $Mode = "bundle"
    $BundleDir = $ScriptDir
} else {
    $Mode = "gitclone"
    $RepoRoot = Split-Path -Parent $ScriptDir
}

Write-Host "==> Mode: $Mode"

# ---------- copy application code ----------
New-Item -ItemType Directory -Force -Path (Join-Path $InstallDir "app") | Out-Null

if ($Mode -eq "bundle") {
    Copy-Item -Recurse -Force -Path (Join-Path $BundleDir "app\*") `
              -Destination (Join-Path $InstallDir "app")
    Copy-Item -Recurse -Force -Path (Join-Path $BundleDir "wheels") `
              -Destination $InstallDir
    if (Test-Path (Join-Path $BundleDir "VERSION")) {
        Copy-Item -Force (Join-Path $BundleDir "VERSION") $InstallDir
    }
} else {
    $AppDst = Join-Path $InstallDir "app"
    foreach ($item in "collector.py","requirements.txt","config.example.yaml") {
        Copy-Item -Force (Join-Path $RepoRoot $item) $AppDst
    }
    foreach ($sub in "core","sources","mappers","sinks","mappings") {
        Copy-Item -Recurse -Force (Join-Path $RepoRoot $sub) $AppDst
    }
}

# ---------- Python runtime + dependencies ----------
if ($Mode -eq "bundle") {
    $PythonExe = Join-Path $InstallDir "python\python.exe"
    if (-not (Test-Path $PythonExe)) {
        Write-Host "==> Extracting portable Python runtime"
        tar -xzf (Join-Path $BundleDir "python-runtime.tar.gz") -C $InstallDir
    }
    Write-Host "==> Installing wheels (offline)"
    & $PythonExe -m pip install `
        --no-index `
        --find-links (Join-Path $InstallDir "wheels") `
        -r (Join-Path $InstallDir "app\requirements.txt")
} else {
    $SysPython = (Get-Command python -ErrorAction SilentlyContinue)?.Source
    if (-not $SysPython) {
        Write-Error "python not found in PATH — install Python 3.x first."
        exit 1
    }
    $VenvDir = Join-Path $InstallDir "venv"
    if (-not (Test-Path $VenvDir)) {
        Write-Host "==> Creating Python venv"
        & python -m venv $VenvDir
    }
    $PythonExe = Join-Path $VenvDir "Scripts\python.exe"
    Write-Host "==> Installing dependencies"
    & $PythonExe -m pip install --upgrade pip -q
    & $PythonExe -m pip install -r (Join-Path $InstallDir "app\requirements.txt") -q
}

# ---------- config ----------
$ConfigPath = Join-Path $InstallDir "config.yaml"
if (-not (Test-Path $ConfigPath)) {
    # Replace relative log/state paths with absolute paths under InstallDir.
    $StateDir = Join-Path $InstallDir "state"
    $LogsDir  = Join-Path $InstallDir "logs"
    (Get-Content (Join-Path $InstallDir "app\config.example.yaml") -Raw) `
        -replace 'dir: \./logs\b',  "dir: $LogsDir" `
        -replace 'dir: logs\b',     "dir: $LogsDir" `
        -replace 'dir: \./state\b', "dir: $StateDir" `
        -replace 'dir: state\b',    "dir: $StateDir" |
        Set-Content $ConfigPath -Encoding UTF8
}
New-Item -ItemType Directory -Force -Path `
    (Join-Path $InstallDir "state"), `
    (Join-Path $InstallDir "logs") | Out-Null

# ---------- Windows service ----------
$ServiceName = "IllumioCollector"

# Remove existing service if present (update scenario)
$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "==> Removing existing service (update)"
    if ($existing.Status -eq "Running") {
        Stop-Service -Name $ServiceName -Force
    }
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
}

$NssmZip = Join-Path $BundleDir "nssm-2.24.zip"
$NssmDir  = Join-Path $InstallDir "nssm"

if (($Mode -eq "bundle") -and (Test-Path $NssmZip)) {
    Write-Host "==> Extracting NSSM"
    if (-not (Test-Path $NssmDir)) {
        Expand-Archive -Path $NssmZip -DestinationPath $NssmDir
    }
    $Nssm = Join-Path $NssmDir "nssm-2.24\win64\nssm.exe"

    Write-Host "==> Registering Windows service (NSSM)"
    & $Nssm install $ServiceName $PythonExe `
        "$InstallDir\app\collector.py --config $InstallDir\config.yaml"
    & $Nssm set $ServiceName AppDirectory   "$InstallDir\app"
    & $Nssm set $ServiceName DisplayName    "Illumio S3 to SIEM Collector"
    & $Nssm set $ServiceName Description    "Pull Illumio PCE logs from S3 and forward to FortiSIEM"
    & $Nssm set $ServiceName AppStdout      "$InstallDir\logs\nssm-stdout.log"
    & $Nssm set $ServiceName AppStderr      "$InstallDir\logs\nssm-stderr.log"
    & $Nssm set $ServiceName AppRotateFiles 1
    & $Nssm set $ServiceName AppRotateBytes 52428800
    & $Nssm set $ServiceName Start          SERVICE_AUTO_START

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Install complete."
    Write-Host " 1. Edit config:     notepad $ConfigPath"
    Write-Host " 2. Start service:   & `"$Nssm`" start $ServiceName"
    Write-Host " 3. Watch logs:      Get-Content $InstallDir\logs\nssm-stdout.log -Wait"
    Write-Host "============================================================"
} else {
    Write-Host "==> Registering Windows service (New-Service)"
    $BinPath = "`"$PythonExe`" `"$InstallDir\app\collector.py`" --config `"$InstallDir\config.yaml`""
    New-Service -Name          $ServiceName `
                -BinaryPathName $BinPath `
                -DisplayName   "Illumio S3 to SIEM Collector" `
                -Description   "Pull Illumio PCE logs from S3 and forward to FortiSIEM" `
                -StartupType   Automatic | Out-Null

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Install complete."
    Write-Host " 1. Edit config:     notepad $ConfigPath"
    Write-Host " 2. Start service:   Start-Service $ServiceName"
    Write-Host " 3. Watch logs:      Get-Content $InstallDir\logs\collector.log -Wait"
    if ($Mode -eq "bundle") {
        Write-Host ""
        Write-Host " NOTE: NSSM not in bundle — stdout/stderr not captured."
        Write-Host "       Download nssm-2.24.zip from https://nssm.cc and re-run."
    }
    Write-Host "============================================================"
}
