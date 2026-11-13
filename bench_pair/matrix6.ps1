$ErrorActionPreference = 'Continue'
Set-Location 'E:\Code\Zig\zgrpc'
$srv = @(
  @{ n='zgrpc-thread'; exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','19450'); port=19450 },
  @{ n='zgrpc-task';   exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','19451','--task'); port=19451 },
  @{ n='zio-iocp';     exe='.\examples\with-zio\zig-out\bin\zio-bench.exe'; a=@('serve','19452','32','16'); port=19452 },
  @{ n='grpc-go';      exe='.\bench_pair\goserver\goserver.exe'; a=@('-addr','127.0.0.1:19453'); port=19453 },
  @{ n='rust-h2';      exe='.\bench_pair\rustserver\target\release\rustserver.exe'; a=@('127.0.0.1:19454'); port=19454 }
)
$rows = @()
foreach ($rep in 1, 2) {
  foreach ($sv in $srv) {
    $proc = Start-Process -FilePath $sv.exe -ArgumentList $sv.a -WindowStyle Hidden -PassThru
    Start-Sleep -Milliseconds 1500
    $o = (& '.\zig-out\bin\bench.exe' --no-serve --port $sv.port --conns 16 --iterations 20000 2>&1 | Out-String)
    $q = [regex]::Match($o, 'qps=([0-9\.]+)')
    $rows += [pscustomobject]@{ rep=$rep; name=$sv.n; qps=$q.Groups[1].Value }
    if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 400
  }
}
$rows | Format-Table -AutoSize | Out-String -Width 200
