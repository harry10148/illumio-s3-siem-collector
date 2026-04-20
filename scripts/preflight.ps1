<#
.SYNOPSIS
    Preflight check — verify the bundle and config before installing on Windows.

.DESCRIPTION
    Extracts Python to a temp directory, installs wheels offline, then runs
    config validation and an optional S3 connectivity test.
    Nothing is written to C:\illumio-collector or the registry.
    Safe to run without Administrator privileges.

.PARAMETER Config
    Path to config.yaml (required).

.PARAMETER TestS3
    Also verify S3 connectivity using credentials in config.

.EXAMPLE
    # Config validation only
    .\preflight.ps1 -Config C:\temp\config.yaml

    # Config + S3 connectivity
    .\preflight.ps1 -Config C:\temp\config.yaml -TestS3
#>
param(
    [Parameter(Mandatory)][string]$Config,
    [switch]$TestS3
)

$ErrorActionPreference = "Stop"

$BundleDir = $PSScriptRoot

# ---------- basic checks ----------
if (-not (Test-Path $Config)) {
    Write-Error "Config file not found: $Config"
    exit 1
}

$RuntimeTar = Join-Path $BundleDir "python-runtime.tar.gz"
if (-not (Test-Path $RuntimeTar)) {
    Write-Error "python-runtime.tar.gz not found. Run this script from inside the extracted bundle directory."
    exit 1
}

# ---------- temp Python runtime ----------
$TempDir = Join-Path $env:TEMP ("illumio-preflight-" + [guid]::NewGuid().ToString("N").Substring(0,8))
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

$cleanup = {
    if (Test-Path $TempDir) { Remove-Item -Recurse -Force $TempDir }
}
# Register cleanup on script exit
try {

Write-Host "==> Extracting Python runtime (temp)"
tar -xzf $RuntimeTar -C $TempDir
$PythonExe = Join-Path $TempDir "python\python.exe"

Write-Host "==> Installing wheels (offline)"
& $PythonExe -m pip install `
    --no-index `
    --find-links (Join-Path $BundleDir "wheels") `
    -r (Join-Path $BundleDir "app\requirements.txt") `
    -q

Write-Host ""
Write-Host "---------- config validation ----------"
& $PythonExe (Join-Path $BundleDir "app\collector.py") `
    --config $Config --dry-run
Write-Host ""

if ($TestS3) {
    Write-Host "---------- S3 connectivity test ----------"

    # Extract S3 fields from config using Python (avoids PowerShell YAML parsing)
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
        (Join-Path $BundleDir "app\s3_log_checker.py"),
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

Write-Host "=========================================="
Write-Host "PASS -- bundle and config look good."
Write-Host "You can now run:  .\install.ps1"
Write-Host "=========================================="

} finally {
    Write-Host "==> Cleaning up temp"
    & $cleanup
}
