<#
.SYNOPSIS
    Preflight check — verify config and S3 connectivity before installing on Windows.

.DESCRIPTION
    Supports two modes (auto-detected):
      bundle   — run from inside an extracted offline bundle
      gitclone — run from inside the repository (scripts\ directory)

    Nothing is written to C:\illumio-collector or the registry.
    Safe to run without Administrator privileges.

.PARAMETER Config
    Path to config.yaml.
    Optional in bundle mode — defaults to app\config.example.yaml if omitted.
    In git-clone mode defaults to config.yaml, then config.example.yaml at repo root.

.PARAMETER TestS3
    Also verify S3 connectivity using credentials in config.

.EXAMPLE
    # Bundle — quick check with bundled example config
    .\preflight.ps1

    # Bundle — use your own config + S3 test
    .\preflight.ps1 -Config C:\temp\config.yaml -TestS3

    # Git clone (from repo root)
    .\scripts\preflight.ps1 -Config config.yaml -TestS3
#>
param(
    [string]$Config = "",
    [switch]$TestS3
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot

# ---------- detect mode ----------
$RuntimeTar = Join-Path $ScriptDir "python-runtime.tar.gz"
if (Test-Path $RuntimeTar) {
    $Mode     = "bundle"
    $AppDir   = Join-Path $ScriptDir "app"
} else {
    $Mode     = "gitclone"
    $RepoRoot = Split-Path -Parent $ScriptDir
    $AppDir   = $RepoRoot
}

Write-Host "==> Mode: $Mode"

# ---------- resolve config ----------
if (-not $Config) {
    if ($Mode -eq "bundle") {
        $Config = Join-Path $AppDir "config.example.yaml"
    } else {
        $try1 = Join-Path $RepoRoot "config.yaml"
        $try2 = Join-Path $RepoRoot "config.example.yaml"
        if    (Test-Path $try1) { $Config = $try1 }
        elseif (Test-Path $try2) { $Config = $try2 }
    }
    if ($Config) {
        Write-Host "==> -Config not specified; using $Config"
    }
}
if (-not $Config -or -not (Test-Path $Config)) {
    Write-Error "Config file not found: '$Config'. Use -Config <path>."
    exit 1
}

$TempDir = Join-Path $env:TEMP ("illumio-preflight-" + [guid]::NewGuid().ToString("N").Substring(0,8))
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

try {

# ---------- Python setup ----------
if ($Mode -eq "bundle") {
    Write-Host "==> Extracting Python runtime (temp)"
    tar -xzf $RuntimeTar -C $TempDir
    $PythonExe = Join-Path $TempDir "python\python.exe"

    Write-Host "==> Installing wheels (offline)"
    & $PythonExe -m pip install `
        --no-index `
        --find-links (Join-Path $ScriptDir "wheels") `
        -r (Join-Path $AppDir "requirements.txt") `
        -q
} else {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    $SysPython = if ($cmd) { $cmd.Source } else { $null }
    if (-not $SysPython) {
        Write-Error "python not found in PATH — install Python 3.x first."
        exit 1
    }
    Write-Host "==> Creating temp venv"
    & python -m venv (Join-Path $TempDir "venv")
    $PythonExe = Join-Path $TempDir "venv\Scripts\python.exe"

    Write-Host "==> Installing dependencies"
    & $PythonExe -m pip install --upgrade pip -q
    & $PythonExe -m pip install -r (Join-Path $AppDir "requirements.txt") -q
}

# ---------- config validation ----------
Write-Host ""
Write-Host "---------- config validation ----------"
& $PythonExe (Join-Path $AppDir "collector.py") --config $Config --dry-run
Write-Host ""

# ---------- S3 connectivity ----------
if ($TestS3) {
    Write-Host "---------- S3 connectivity test ----------"

    $ExtractScript = @'
import sys, yaml
with open(sys.argv[1]) as f:
    cfg = yaml.safe_load(f)
aws = cfg.get("aws", {})
src = cfg.get("source", {})
print(aws.get("access_key",""))
print(aws.get("secret_key",""))
print(src.get("bucket",""))
print(src.get("fqdn",""))
print(str(src.get("org_id","")))
print(aws.get("region","") or "")
'@
    $vals = & $PythonExe -c $ExtractScript $Config
    $AK, $SK, $Bucket, $Fqdn, $OrgId, $Region = $vals

    $CheckerArgs = @(
        (Join-Path $AppDir "s3_log_checker.py"),
        "--bucket",     $Bucket,
        "--fqdn",       $Fqdn,
        "--org-id",     $OrgId,
        "--access-key", $AK,
        "--secret-key", $SK
    )
    if ($Region) { $CheckerArgs += "--region", $Region }

    & $PythonExe @CheckerArgs
    Write-Host ""
}

# ---------- result ----------
$NextStep = if ($Mode -eq "bundle") { ".\install.ps1" } else { ".\scripts\install.ps1" }

Write-Host "=========================================="
Write-Host "PASS -- config and dependencies look good."
Write-Host "You can now run:  $NextStep"
Write-Host "=========================================="

} finally {
    Write-Host "==> Cleaning up temp"
    if (Test-Path $TempDir) { Remove-Item -Recurse -Force $TempDir }
}
