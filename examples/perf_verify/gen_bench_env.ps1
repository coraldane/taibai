param(
    [int]$Rows = 100000,
    [string]$Root = (Join-Path $env:TEMP 'taibai_bench'),
    [int]$Port = 5153
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path $Root)) { New-Item -ItemType Directory -Path $Root | Out-Null }

$csvOut = Join-Path $Root 'ticks.csv'
if (Test-Path $csvOut) { Remove-Item $csvOut -Force }
$sb = New-Object System.Text.StringBuilder
$prices = @(100.25, 101.50, 99.75, 103.10, 102.00, 98.85, 105.25, 100.00)
for ($i = 0; $i -lt $Rows; $i++) {
    $sym = 'SZ{0:D6}' -f ($i % 100)
    $t = 93000 + ($i % 10000)
    $p = $prices[$i % 8]
    $px = '{0:F2}' -f $p
    $px2 = '{0:F2}' -f ($p + 0.05)
    $vol = ($i % 900 + 100)
    $ts = '2026-09-11 09:{0:D2}:{1:D2}.{2:D3}' -f (30 + ($i % 20)), ($i % 60), ($i % 1000)
    [void]$sb.Append($sym).Append(',20260911,').Append($t)
    foreach ($k in 1..10) { [void]$sb.Append(',').Append($px) }
    [void]$sb.Append(',').Append($vol * 10)
    [void]$sb.Append(',').Append($vol * 1000).Append(',').Append($px2)
    foreach ($k in 1..5) { [void]$sb.Append(',').Append($px2).Append(',').Append($vol) }
    foreach ($k in 1..5) { [void]$sb.Append(',').Append($px).Append(',').Append($vol) }
    [void]$sb.Append(',').Append($ts).Append("`r`n")
    if ($i % 20000 -eq 19999) {
        [System.IO.File]::AppendAllText($csvOut, $sb.ToString())
        $sb.Clear() | Out-Null
    }
}
if ($sb.Length -gt 0) { [System.IO.File]::AppendAllText($csvOut, $sb.ToString()) }

$rootForward = $Root.Replace('\', '/')
$dataDir = Join-Path $rootForward 'data'
$logFile = Join-Path $rootForward 'taibai.log'
$yaml = @"
server:
  host: "127.0.0.1"
  port: $Port
  max_connections: 128
  max_concurrent_requests: 16

storage:
  data_dir: "$dataDir"
  default_database: "main"
  min_segment_seal_size_mb: 500
  max_segment_size_mb: 2048
  hot_store_memory_limit_mb: 2048
  row_group_target_rows: 65536
  reader_cache_capacity: 256

buffer:
  query_memory_limit_mb: 2048
  execution_threads: 8
  max_parallel_row_groups: 8
  global_io_permits: 4
  global_cpu_permits: 8

compression:
  enabled: true
  bulk_zstd_level: 3

log:
  level: "warn"
  file: "$logFile"
  insert_profile: true
"@
Set-Content -Path (Join-Path $Root 'bench.yaml') -Value $yaml
$lineCount = ([System.IO.File]::ReadAllLines($csvOut)).Count
"WROTE $lineCount lines to $csvOut"
"WROTE bench.yaml with data_dir $dataDir on port $Port"
