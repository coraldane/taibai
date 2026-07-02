param(
    [string]$InstallDir = "C:\Program Files\Taibai",
    [string]$DataDir = "",
    [string]$ConfigDir = "",
    [string]$ServiceName = "TaibaiDB",
    [string]$DisplayName = "Taibai Database",
    [int]$Port = 5151,
    [string]$SourceDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Administrator privileges are required"
}

$binDir = Join-Path $InstallDir "bin"
$confDir = Join-Path $InstallDir "conf"
$dataDirDefault = Join-Path $InstallDir "data"
$logDir = Join-Path $InstallDir "logs"
if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = $dataDirDefault
}
if ([string]::IsNullOrWhiteSpace($ConfigDir)) {
    $ConfigDir = $confDir
}
$configPath = Join-Path $ConfigDir "windows.yaml"
$targetExe = Join-Path $binDir "taibai.exe"
$packagedExe = Join-Path $SourceDir "bin\taibai.exe"
$buildExe = Join-Path $SourceDir "target\release\taibai.exe"
$sourceExe = $null

if (Test-Path $packagedExe) {
    $sourceExe = $packagedExe
} elseif (Test-Path $buildExe) {
    $sourceExe = $buildExe
}

if (-not $sourceExe) {
    throw "Binary not found. Checked: $packagedExe and $buildExe"
}

New-Item -ItemType Directory -Force -Path $binDir, $confDir, $logDir, $ConfigDir, $DataDir | Out-Null
if ((Resolve-Path $sourceExe).Path -ne (Resolve-Path $targetExe -ErrorAction SilentlyContinue).Path) {
    Copy-Item $sourceExe $targetExe -Force
}

$config = @"
server:
  host: ""0.0.0.0""
  port: $Port
  max_message_size: 67108864
storage:
  data_dir: ""$($DataDir.Replace('\', '/'))""
  default_database: ""main""
  flush_threshold_rows: 65536
  min_segment_seal_size_mb: 500
buffer:
  batch_size: 4096
compression:
  enabled: true
tls:
  enabled: false
  cert_path: """"
  key_path: """"
auto_compact:
  enabled: true
  check_interval_secs: 60
  idle_threshold_secs: 30
  min_segments: 4
  min_deleted_rows: 100000
  max_tables_per_run: 1
log:
  level: ""info""
  file: ""$((Join-Path $logDir "taibai.log").Replace('\', '/'))""
"@
Set-Content -Path $configPath -Value $config -Encoding UTF8

& $targetExe --install-service --service-name $ServiceName --service-display-name $DisplayName --config $configPath
Write-Host "Taibai installed to $InstallDir"
Write-Host "Data directory: $DataDir"
Write-Host "Config file: $configPath"
Write-Host "Service: $ServiceName"
Write-Host "Start with: Start-Service $ServiceName"
