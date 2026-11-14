$ErrorActionPreference = 'Continue'
Set-Location 'E:\Code\Zig\zgrpc'
$srv = @(
  @{ n='zgrpc-thread'; exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','20201'); port=20201 },
  @{ n='zgrpc-task';   exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','20202','--task'); port=20202 },
  @{ n='zio-iocp';     exe='.\examples\with-zio\zig-out\bin\zio-bench.exe'; a=@('serve','20203','32','16','4','64','0','0','1'); port=20203 },
  @{ n='grpc-go';      exe='.\bench_pair\goserver\goserver.exe'; a=@('-addr','127.0.0.1:20204'); port=20204 },
  @{ n='rust-h2';      exe='.\bench_pair\rustserver\target\release\rustserver.exe'; a=@('127.0.0.1:20205'); port=20205 }
)
$rows = @()
foreach ($sv in $srv) {
  $proc = Start-Process -FilePath $sv.exe -ArgumentList $sv.a -WindowStyle Hidden -PassThru
  Start-Sleep -Milliseconds 1500
  foreach ($b in 1, 32) {
    $o = (& '.\zig-out\bin\bench.exe' --no-serve --port $sv.port --batch $b --iterations 2000 2>&1 | Out-String)
    $m = [regex]::Match($o, 'avg_ms=([0-9\.]+)')
    $ms = [double]$m.Groups[1].Value
    $rows += [pscustomobject]@{ server=$sv.n; streams=$b; round_ms=$ms; per_req_us=[math]::Round($ms * 1000 / $b, 1) }
  }
  if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }
  Start-Sleep -Milliseconds 400
}
$rows | Format-Table -AutoSize | Out-String -Width 120
