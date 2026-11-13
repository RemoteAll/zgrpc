$ErrorActionPreference = 'Stop'
Set-Location 'E:\Code\Zig\zgrpc'
$ghz = 'C:\Users\qcjxb\go\bin\ghz.exe'
$srv = @(
  @{ n='zgrpc-thread'; exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','19420'); port=19420 },
  @{ n='zgrpc-task';   exe='.\zig-out\bin\bench.exe'; a=@('--serve-only','--port','19421','--task'); port=19421 },
  @{ n='zio-iocp';     exe='.\examples\with-zio\zig-out\bin\zio-bench.exe'; a=@('serve','19422','32','16'); port=19422 },
  @{ n='grpc-go';      exe='.\bench_pair\goserver\goserver.exe'; a=@('-addr','127.0.0.1:19423'); port=19423 },
  @{ n='rust-h2';      exe='.\bench_pair\rustserver\target\release\rustserver.exe'; a=@('127.0.0.1:19424'); port=19424 }
)
$rows = @()
foreach ($rep in 1, 2) {
  foreach ($sv in $srv) {
    $p = Start-Process -FilePath $sv.exe -ArgumentList $sv.a -WindowStyle Hidden -PassThru
    Start-Sleep -Milliseconds 1500
    $o = (& $ghz --insecure --proto 'bench_pair\bench.proto' --call 'bench.Bench.Echo' -D 'bench_pair\payload.json' -c 64 --connections 16 -z 8s ("127.0.0.1:" + $sv.port) 2>&1 | Out-String)
    $q = [regex]::Match($o, 'Requests/sec:\s+([0-9\.]+)')
    $m = [regex]::Match($o, '50 %\s+in\s+([0-9\.]+)\s+ms')
    $rows += [pscustomobject]@{ rep=$rep; name=$sv.n; qps=$q.Groups[1].Value; p50_ms=$m.Groups[1].Value }
    if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 400
  }
}
$rows | Format-Table -AutoSize | Out-String -Width 200
