<#
.SYNOPSIS
    Install the Illumio S3 -> SIEM Collector from an offline bundle on Windows.
    Must be run as Administrator from inside the extracted bundle directory.
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

$BundleDir = $PSScriptRoot

Write-Host "==> Copying bundle to $InstallDir"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Recurse -Force -Path (Join-Path $BundleDir "app"), `
                                 (Join-Path $BundleDir "wheels") `
                          -Destination $InstallDir
Copy-Item -Force (Join-Path $BundleDir "VERSION") $InstallDir

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

Write-Host "==> Preparing config"
$ConfigPath = Join-Path $InstallDir "config.yaml"
if (-not (Test-Path $ConfigPath)) {
    Copy-Item (Join-Path $InstallDir "app\config.example.yaml") $ConfigPath
}

New-Item -ItemType Directory -Force -Path `
    (Join-Path $InstallDir "state"), `
    (Join-Path $InstallDir "logs") | Out-Null

Write-Host "==> Extracting NSSM"
$NssmDir = Join-Path $InstallDir "nssm"
if (-not (Test-Path $NssmDir)) {
    Expand-Archive -Path (Join-Path $BundleDir "nssm-2.24.zip") `
                   -DestinationPath $NssmDir
}
$Nssm = Join-Path $NssmDir "nssm-2.24\win64\nssm.exe"

Write-Host "==> Registering Windows service"
$ServiceName = "IllumioCollector"
& $Nssm install $ServiceName $PythonExe `
    "$InstallDir\app\collector.py --config $InstallDir\config.yaml"
& $Nssm set $ServiceName AppDirectory      "$InstallDir\app"
& $Nssm set $ServiceName DisplayName       "Illumio S3 to SIEM Collector"
& $Nssm set $ServiceName Description       "Pull Illumio PCE logs from S3 and forward to FortiSIEM"
& $Nssm set $ServiceName AppStdout         "$InstallDir\logs\nssm-stdout.log"
& $Nssm set $ServiceName AppStderr         "$InstallDir\logs\nssm-stderr.log"
& $Nssm set $ServiceName AppRotateFiles    1
& $Nssm set $ServiceName AppRotateBytes    52428800
& $Nssm set $ServiceName Start             SERVICE_AUTO_START

Write-Host ""
Write-Host "============================================================"
Write-Host "Install complete."
Write-Host " 1. Edit the config:   notepad $ConfigPath"
Write-Host " 2. Start the service: & `"$Nssm`" start $ServiceName"
Write-Host " 3. Watch the logs:    Get-Content $InstallDir\logs\collector.log -Wait"
Write-Host "============================================================"
