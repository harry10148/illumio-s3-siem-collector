<#
.SYNOPSIS
    Install the Illumio S3 -> SIEM Collector on Windows.

.DESCRIPTION
    Supports two modes (auto-detected):

    Bundle mode   — run from inside an extracted offline bundle
                    Administrator PowerShell inside bundle dir:
                        .\install.ps1

    Git-clone mode — run from the repository's scripts\ directory
                    Administrator PowerShell at repo root:
                        .\scripts\install.ps1

.PARAMETER InstallDir
    Installation directory (default: C:\illumio-collector).

.PARAMETER ServiceAccount
    Windows account to run the service under.
    Default: "NT AUTHORITY\NetworkService" (recommended least privilege default).
    Use "LocalSystem" only if you explicitly need full local system access.
    You can also use "NT AUTHORITY\LocalService" for stricter local permissions,
    or "DOMAIN\username" for a domain account (NSSM only — requires
    -ServicePassword as well).

.PARAMETER ServicePassword
    Password for -ServiceAccount when using a domain/local user account.
    Leave empty for built-in accounts (LocalSystem, NetworkService).
#>
param(
    [string]$InstallDir      = "$env:ProgramFiles\illumio-collector",
    [string]$ServiceAccount  = "NT AUTHORITY\NetworkService",
    [string]$ServicePassword = ""
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Error "This script must be run as Administrator."
    exit 1
}

$ScriptDir = $PSScriptRoot
$BuiltInAccounts = @("LocalSystem", "NT AUTHORITY\NetworkService", "NT AUTHORITY\LocalService")
$IsBuiltInServiceAccount = $BuiltInAccounts -contains $ServiceAccount

if (-not $IsBuiltInServiceAccount -and [string]::IsNullOrEmpty($ServicePassword)) {
    Write-Error "ServicePassword is required when ServiceAccount is not a built-in account."
    exit 1
}

# ---------- detect mode ----------
if (Test-Path (Join-Path $ScriptDir "python-runtime.tar.gz")) {
    $Mode      = "bundle"
    $BundleDir = $ScriptDir
} else {
    $Mode     = "gitclone"
    $RepoRoot = Split-Path -Parent $ScriptDir
}

Write-Host "==> Mode: $Mode  service-account: $ServiceAccount"

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
    # Make uninstall.ps1 available from install dir
    if (Test-Path (Join-Path $BundleDir "uninstall.ps1")) {
        Copy-Item -Force (Join-Path $BundleDir "uninstall.ps1") $InstallDir
    }
} else {
    $AppDst = Join-Path $InstallDir "app"
    foreach ($item in "collector.py","s3_log_checker.py","requirements.txt","config.example.yaml") {
        Copy-Item -Force (Join-Path $RepoRoot $item) $AppDst
    }
    foreach ($sub in "core","sources","mappers","sinks","mappings") {
        Copy-Item -Recurse -Force (Join-Path $RepoRoot $sub) $AppDst
    }
    if (Test-Path (Join-Path $RepoRoot "scripts\uninstall.ps1")) {
        Copy-Item -Force (Join-Path $RepoRoot "scripts\uninstall.ps1") $InstallDir
    }
}

# ---------- save install metadata ----------
@"
service_account=$ServiceAccount
install_mode=$Mode
installed=$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ')
"@ | Set-Content (Join-Path $InstallDir "INSTALL_META") -Encoding UTF8

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
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    $SysPython = if ($cmd) { $cmd.Source } else { $null }
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
    $StateDir = Join-Path $InstallDir "state"
    $LogsDir  = Join-Path $InstallDir "logs"
    (Get-Content (Join-Path $InstallDir "app\config.example.yaml") -Raw -Encoding UTF8) `
        -replace 'dir: \./logs\b',  "dir: $LogsDir" `
        -replace 'dir: logs\b',     "dir: $LogsDir" `
        -replace 'dir: \./state\b', "dir: $StateDir" `
        -replace 'dir: state\b',    "dir: $StateDir" `
        -replace '"/var/log/illumio-collector/', "`"$LogsDir\" |
        Set-Content $ConfigPath -Encoding UTF8
}
New-Item -ItemType Directory -Force -Path `
    (Join-Path $InstallDir "state"), `
    (Join-Path $InstallDir "logs") | Out-Null

# ---------- Windows service ----------
$ServiceName = "IllumioCollector"

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "==> Removing existing service (update)"
    if ($existing.Status -in @("Running", "Paused")) {
        sc.exe stop $ServiceName | Out-Null
        Start-Sleep -Seconds 2
    }
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
}

$NssmZip = if ($Mode -eq "bundle") { Join-Path $BundleDir "nssm-2.24.zip" } else { "" }
$NssmDir = Join-Path $InstallDir "nssm"

if ($NssmZip -and (Test-Path $NssmZip)) {
    Write-Host "==> Extracting NSSM"
    if (-not (Test-Path $NssmDir)) { Expand-Archive -Path $NssmZip -DestinationPath $NssmDir }
    $Nssm = Join-Path $NssmDir "nssm-2.24\win64\nssm.exe"

    Write-Host "==> Registering Windows service (NSSM, account: $ServiceAccount)"
    & $Nssm install $ServiceName $PythonExe `
        "`"$InstallDir\app\collector.py`" --config `"$InstallDir\config.yaml`""
    & $Nssm set $ServiceName AppDirectory   "$InstallDir\app"
    & $Nssm set $ServiceName DisplayName    "Illumio S3 to SIEM Collector"
    & $Nssm set $ServiceName Description    "Pull Illumio PCE logs from S3 and forward to SIEM"
    & $Nssm set $ServiceName AppStdout      "$InstallDir\logs\nssm-stdout.log"
    & $Nssm set $ServiceName AppStderr      "$InstallDir\logs\nssm-stderr.log"
    & $Nssm set $ServiceName AppRotateFiles 1
    & $Nssm set $ServiceName AppRotateBytes 52428800
    & $Nssm set $ServiceName Start          SERVICE_AUTO_START
    if ($ServiceAccount -ne "LocalSystem") {
        if ($IsBuiltInServiceAccount) {
            & $Nssm set $ServiceName ObjectName $ServiceAccount
        } else {
            & $Nssm set $ServiceName ObjectName $ServiceAccount $ServicePassword
        }
    }

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Install complete.  (account: $ServiceAccount)"
    Write-Host " Uninstall: & '$InstallDir\uninstall.ps1'"
    Write-Host " 1. Edit config:   notepad $ConfigPath"
    Write-Host " 2. Start service: & `"$Nssm`" start $ServiceName"
    Write-Host " 3. Watch logs:    Get-Content $InstallDir\logs\nssm-stdout.log -Wait"
    Write-Host "============================================================"
} else {
    Write-Error @"
ERROR: NSSM (Non-Sucking Service Manager) is required but was not found.

Plain Python scripts cannot run as Windows services without a wrapper — New-Service
will always fail with Error 1053.

To fix:
  1. Download nssm-2.24.zip (search for 'nssm 2.24 download' or get it from nssm.cc)
  2. Place nssm-2.24.zip in the same folder as install.ps1 ($ScriptDir)
  3. Re-run install.ps1

If you built the bundle with build_offline_bundle.ps1, re-run the build on a
machine that can reach nssm.cc so NSSM is included automatically.
"@
    exit 1
}
