<#
.SYNOPSIS
    Uninstall the Illumio S3 -> SIEM Collector from Windows.

.DESCRIPTION
    Stops and removes the Windows service and deletes the install directory.

    By default, config.yaml and state/ are preserved so a reinstall can
    resume without data loss.  Use -Purge to remove everything.

.PARAMETER InstallDir
    Installation directory (default: C:\illumio-collector)

.PARAMETER Purge
    Also remove config.yaml and state/ (checkpoint files).

.EXAMPLE
    # Standard uninstall — config and state preserved
    Administrator PS> .\uninstall.ps1

    # Full removal
    Administrator PS> .\uninstall.ps1 -Purge
#>
param(
    [string]$InstallDir = "C:\illumio-collector",
    [switch]$Purge
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Error "This script must be run as Administrator."
    exit 1
}

$ServiceName = "IllumioCollector"

# ---------- stop and remove service ----------
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
    if ($svc.Status -eq "Running") {
        Write-Host "==> Stopping service"
        Stop-Service -Name $ServiceName -Force
        Start-Sleep -Seconds 2
    }

    # Try NSSM remove first (cleaner), fall back to sc.exe
    $Nssm = Join-Path $InstallDir "nssm\nssm-2.24\win64\nssm.exe"
    if (Test-Path $Nssm) {
        Write-Host "==> Removing service (NSSM)"
        & $Nssm remove $ServiceName confirm | Out-Null
    } else {
        Write-Host "==> Removing service (sc.exe)"
        sc.exe delete $ServiceName | Out-Null
    }
    Start-Sleep -Seconds 1
} else {
    Write-Host "==> Service '$ServiceName' not found — skipping"
}

# ---------- preserve config and state (default) ----------
$ConfigPath = Join-Path $InstallDir "config.yaml"
$StatePath  = Join-Path $InstallDir "state"
$LogsPath   = Join-Path $InstallDir "logs"

$savedConfig = $null
$savedState  = $null

if (-not $Purge) {
    # Move config and state out before removing the install dir
    $TempSave = Join-Path $env:TEMP "illumio-collector-save"
    New-Item -ItemType Directory -Force -Path $TempSave | Out-Null

    if (Test-Path $ConfigPath) {
        Copy-Item -Force $ConfigPath $TempSave
        $savedConfig = Join-Path $TempSave "config.yaml"
    }
    if (Test-Path $StatePath) {
        Copy-Item -Recurse -Force $StatePath $TempSave
        $savedState = Join-Path $TempSave "state"
    }
}

# ---------- remove install directory ----------
if (Test-Path $InstallDir) {
    Write-Host "==> Removing $InstallDir"
    Remove-Item -Recurse -Force $InstallDir
}

# ---------- restore preserved files ----------
if (-not $Purge) {
    if ($savedConfig -or $savedState) {
        New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
        if ($savedConfig) {
            Copy-Item -Force $savedConfig $InstallDir
            Write-Host "==> Preserved: $ConfigPath"
        }
        if ($savedState) {
            Copy-Item -Recurse -Force $savedState $InstallDir
            Write-Host "==> Preserved: $StatePath"
        }
        Remove-Item -Recurse -Force $TempSave
    }

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Uninstall complete."
    Write-Host ""
    Write-Host "Preserved files (safe to reinstall later):"
    if ($savedConfig) { Write-Host "  $ConfigPath" }
    if ($savedState)  { Write-Host "  $StatePath\" }
    Write-Host ""
    Write-Host "To also remove these, re-run with -Purge:"
    Write-Host "  .\uninstall.ps1 -Purge"
    Write-Host "============================================================"
} else {
    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Uninstall complete (purged — all data removed)."
    Write-Host "============================================================"
}
