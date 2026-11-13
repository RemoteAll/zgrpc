$ErrorActionPreference = 'Continue'
Set-Location 'E:\Code\Zig\zgrpc'
$rows = @()
# 同一套服务端逻辑（zgrpc grpc.Server）、同一 job_mode=thread_pool、同一 pool 上限 64，
# 只换 IO 后端：Threaded vs zio(IOCP)。
foreach ($delay in 0, 1000) {
  foreach ($b in 32, 100) {
    # A: std.Io.Threaded（bench 自带服务端，--task 自动建池）
    $o = (& '.\zig-out\bin\bench.exe' --batch $b --iterations 20 --handler-delay-us $delay --port (19700 + $b) --task --max-streams 1024 2>&1 | Out-String)
    $m = [regex]::Match($o, 'avg_ms=([0-9\.]+)')
    $rows += [pscustomobject]@{ backend='Threaded'; delay_us=$delay; streams=$b; avg_ms=$m.Groups[1].Value }

    # B: zio(IOCP)，arg9=1 → job_mode=thread_pool
    $proc = Start-Process -FilePath '.\examples\with-zio\zig-out\bin\zio-bench.exe' -ArgumentList @('serve', (19800 + $b), '32', '16', '4', '64', $delay, '0', '1') -WindowStyle Hidden -PassThru
    Start-Sleep -Milliseconds 1500
    $o2 = (& '.\zig-out\bin\bench.exe' --no-serve --port (19800 + $b) --batch $b --iterations 20 2>&1 | Out-String)
    $m2 = [regex]::Match($o2, 'avg_ms=([0-9\.]+)')
    $rows += [pscustomobject]@{ backend='zio(IOCP)'; delay_us=$delay; streams=$b; avg_ms=$m2.Groups[1].Value }
    if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 400
  }
}
$rows | Format-Table -AutoSize | Out-String -Width 120
