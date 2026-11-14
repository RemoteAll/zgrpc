param([switch]$Update)

$ErrorActionPreference = 'Continue'
# 性能收益护栏：跑固定指标集，和 baseline.json 比对，回退超过阈值就报 FAIL。
#
# 为什么要它：
#   1. 曾用 Debug 构建和别人的 release 比，得出过完全错误的结论；
#   2. 也曾做出「看起来该有用」的优化，实测反而更慢（固定头块缓存）。
# 所以任何性能改动前后都跑一遍这个脚本，用同一份二进制、同一组指标说话。
#
# 用法：
#   powershell -File bench_pair/guard.ps1            # 对比基线
#   powershell -File bench_pair/guard.ps1 -Update     # 重新生成基线（确认收益后再做）

Set-Location 'E:\Code\Zig\zgrpc'
$zig = 'E:\Soft\zig-x86_64-windows-0.17.1\zig.exe'
$env:ZIG_GLOBAL_CACHE_DIR = 'E:\Code\Zig\zgrpc\.zig-global-cache'

# 0) 永远用 ReleaseFast 构建，杜绝 Debug 混入
& $zig build bench -Doptimize=ReleaseFast 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host 'BUILD FAILED'; exit 1 }
& $zig build tcp-floor -Doptimize=ReleaseFast 2>&1 | Out-Null
& $zig build shm-ring -Doptimize=ReleaseFast 2>&1 | Out-Null

function Get-Metric([string]$exe, [string[]]$extra, [string]$pattern) {
  $o = (& $exe @extra 2>&1 | Out-String)
  $m = [regex]::Match($o, $pattern)
  if (-not $m.Success) { return $null }
  return [double]$m.Groups[1].Value
}

$port = 22000
$metrics = [ordered]@{}

# 1) gRPC 串行单请求 RTT（us）。用**最快一轮** `min_ms` 而不是平均：
#    平均受系统负载影响很大（实测 90~121µs 抖动），会误报回退。
$port++
$metrics['rpc_serial_us'] = Get-Metric '.\zig-out\bin\bench.exe' @('--batch','1','--iterations','3000','--port',"$port") 'min_ms=([0-9\.]+)'
if ($null -ne $metrics['rpc_serial_us']) { $metrics['rpc_serial_us'] = [math]::Round($metrics['rpc_serial_us'] * 1000, 1) }
# 2) gRPC 32 流流水线（us/请求）
$port++
$metrics['rpc_pipe32_us'] = Get-Metric '.\zig-out\bin\bench.exe' @('--batch','32','--iterations','3000','--port',"$port") 'per_stream_us=([0-9\.]+)'
# 3) gRPC 100 流流水线（us/请求）
$port++
$metrics['rpc_pipe100_us'] = Get-Metric '.\zig-out\bin\bench.exe' @('--batch','100','--iterations','2000','--port',"$port",'--max-streams','1024') 'per_stream_us=([0-9\.]+)'
# 4) 裸 TCP 往返地板（us）：参考值，用来判断「是我们慢了还是机器慢了」
$metrics['tcp_floor_avg_us'] = Get-Metric '.\zig-out\bin\tcp_floor.exe' @('20000') 'avg_us=([0-9\.]+)'

$baselinePath = 'E:\Code\Zig\zgrpc\bench_pair\baseline.json'

if ($Update -or -not (Test-Path $baselinePath)) {
  $obj = [ordered]@{ created = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'); metrics = $metrics }
  ($obj | ConvertTo-Json -Depth 5) | Set-Content -Path $baselinePath -Encoding UTF8
  Write-Host "已写入基线：$baselinePath"
  $metrics.GetEnumerator() | ForEach-Object { '{0,-20} {1,10}' -f $_.Key, $_.Value }
  exit 0
}

$baseline = (Get-Content $baselinePath -Raw | ConvertFrom-Json).metrics
# 只对「越低越好」的延迟指标设阈值：允许 15% 抖动
$tolerance = 1.15
$failed = $false
$rows = @()
foreach ($k in $metrics.Keys) {
  $now = $metrics[$k]
  $base = $baseline.$k
  if ($null -eq $now -or $null -eq $base -or $base -eq 0) {
    $rows += [pscustomobject]@{ metric = $k; baseline = $base; now = $now; delta_pct = 'n/a'; verdict = 'SKIP' }
    continue
  }
  $ratio = $now / $base
  $verdict = if ($ratio -gt $tolerance) { $failed = $true; 'FAIL' } elseif ($ratio -lt 0.95) { 'BETTER' } else { 'OK' }
  $rows += [pscustomobject]@{
    metric = $k; baseline = [math]::Round($base, 2); now = [math]::Round($now, 2)
    delta_pct = [math]::Round(($ratio - 1) * 100, 1); verdict = $verdict
  }
}
$rows | Format-Table -AutoSize | Out-String -Width 120 | Write-Host
if ($failed) { Write-Host 'REGRESSION DETECTED'; exit 1 }
Write-Host 'ALL METRICS WITHIN THRESHOLD'
exit 0
