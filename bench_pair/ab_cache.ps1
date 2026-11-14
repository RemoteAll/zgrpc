$ErrorActionPreference = 'Continue'
Set-Location 'E:\Code\Zig\zgrpc'
$rows = @()
foreach ($rep in 1, 2, 3) {
  foreach ($exe in 'bench_base', 'bench_cache') {
    foreach ($b in 32, 100) {
      $o = (& ".\zig-out\bin\$exe.exe" --batch $b --iterations 4000 --port (21800 + $b + $rep) 2>&1 | Out-String)
      $m = [regex]::Match($o, 'per_stream_us=([0-9\.]+)')
      $mn = [regex]::Match($o, 'min_ms=([0-9\.]+)')
      $rows += [pscustomobject]@{
        rep = $rep; exe = $exe; streams = $b
        per_req_us = [double]$m.Groups[1].Value
        min_round_ms = [double]$mn.Groups[1].Value
      }
    }
  }
}
$rows | Format-Table -AutoSize | Out-String -Width 120
$rows | Group-Object exe, streams | ForEach-Object {
  $avg = ($_.Group | Measure-Object per_req_us -Average).Average
  $mn = ($_.Group | Measure-Object min_round_ms -Minimum).Minimum
  [pscustomobject]@{ key = $_.Name; avg_per_req_us = [math]::Round($avg, 2); best_min_round_ms = $mn }
} | Format-Table -AutoSize | Out-String -Width 120
