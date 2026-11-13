# 跨语言对拍（稳态）：交替测量两端，压制机器状态漂移；每档取中位数。
#
# 为什么每档 N 不同：
#   c=1 是**延迟 bound**（约 270us 一次往返），N=100000 要跑近 30 秒；
#   高并发档是**吞吐 bound**，几秒就跑完。用同一 N 会让脚本看起来像卡死。
#
# 用法：
#   .\steady-matrix.ps1
#   .\steady-matrix.ps1 -Runs 5
#   .\steady-matrix.ps1 -Scale 2          # 所有档的 N 翻倍（更稳、更慢）
param(
    [int[]]$Ports = @(18054, 18055),
    [int]$Runs = 3,
    [double]$Scale = 1.0
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$GhzExe = 'C:\Users\qcjxb\go\bin\ghz.exe'
$label = @{ 18054 = 'zgrpc'; 18055 = 'go' }

function Invoke-Ghz([string]$Port, [int]$Conc, [int]$Conns, [int]$N) {
    $out = & $GhzExe --insecure --proto bench.proto --call bench.Bench.Echo -D payload.json `
        -c $Conc --connections $Conns -n $N "127.0.0.1:$Port" 2>&1 | Out-String
    [pscustomobject]@{
        Rps = [double][regex]::Match($out, 'Requests/sec:\s*([\d.]+)').Groups[1].Value
        P50 = [regex]::Match($out, '50 % in ([\d.]+ \w+)').Groups[1].Value
        P99 = [regex]::Match($out, '99 % in ([\d.]+ \w+)').Groups[1].Value
        Ok  = [regex]::Match($out, '\[OK\]\s*(\d+)').Groups[1].Value
    }
}

function Med([double[]]$v) {
    $s = @($v | Sort-Object)
    return $s[[int][math]::Floor($s.Count / 2)]
}

# 每档单独给基准 N（已乘 Scale）
#
# 关于连接数：本机 16 逻辑核，**压测器与服务端同机**。
#   连接数 > 核数时（如 64/64 ≈ 128 个线程抢 16 核），两端 CPU/req 都会从 ~9us 暴涨到 ~110us，
#   实测 grpc-go 与 zgrpc 几乎一样（111.6 vs 109.8 us），说明那档量的是**操作系统在过载下的开销**，
#   不是应用效率。所以公平档取 conns <= 核数，64/64 只作为「过载参考」，不参与结论。
$cases = @(
    @{ Name = 'conns=1  c=1  '; Conc = 1;  Conns = 1;  N = [int](20000  * $Scale) },
    @{ Name = 'conns=1  c=8  '; Conc = 8;  Conns = 1;  N = [int](60000  * $Scale) },
    @{ Name = 'conns=1  c=64 '; Conc = 64; Conns = 1;  N = [int](100000 * $Scale) },
    @{ Name = 'conns=8  c=8  '; Conc = 8;  Conns = 8;  N = [int](100000 * $Scale) },
    @{ Name = 'conns=16 c=16 '; Conc = 16; Conns = 16; N = [int](100000 * $Scale) },
    @{ Name = 'conns=64 c=64 '; Conc = 64; Conns = 64; N = [int](100000 * $Scale) }
)

Write-Host ("每档 {0} 次，取中位数；端口 {1}" -f $Runs, ($Ports -join ','))

$rows = @()
foreach ($case in $cases) {
    $caseOrigin = Get-Date
    # 预热一轮（不进统计）
    foreach ($port in $Ports) {
        [void](Invoke-Ghz $port $case.Conc $case.Conns ([int]($case.N / 10)))
    }

    $acc = @{}
    foreach ($port in $Ports) { $acc[$port] = @() }
    for ($i = 1; $i -le $Runs; $i++) {
        foreach ($port in $Ports) {
            $m = Invoke-Ghz $port $case.Conc $case.Conns $case.N
            $acc[$port] += $m
            Write-Host ("  [{0}] {1,-5} run{2}: {3,7} rps  p50={4}  p99={5}  ok={6}" -f `
                $case.Name, $label[$port], $i, [int]$m.Rps, $m.P50, $m.P99, $m.Ok)
        }
    }
    foreach ($port in $Ports) {
        $list = $acc[$port]
        $rows += [pscustomobject]@{
            Case  = $case.Name
            Impl  = $label[$port]
            QPS   = [int](Med ([double[]]($list | ForEach-Object { $_.Rps })))
            P50   = $list[0].P50
            P99   = $list[0].P99
            AllOk = -not ($list | Where-Object { $_.Ok -ne $case.N })
        }
    }
    Write-Host ("  [{0}] 本档耗时 {1:N0}s" -f $case.Name, ((Get-Date) - $caseOrigin).TotalSeconds)
}

Write-Host ''
$rows | Format-Table -AutoSize | Out-String | Write-Host

if ($Ports.Count -eq 2) {
    Write-Host '相对差距（zgrpc / go - 1）：'
    foreach ($case in $cases) {
        $z = $rows | Where-Object { $_.Case -eq $case.Name -and $_.Impl -eq 'zgrpc' }
        $g = $rows | Where-Object { $_.Case -eq $case.Name -and $_.Impl -eq 'go' }
        if ($z -and $g -and $g.QPS -gt 0) {
            $d = [math]::Round(($z.QPS / [double]$g.QPS - 1) * 100, 1)
            $mark = if ($d -ge 0) { '+' } else { '' }
            Write-Host ("  {0}  zgrpc={1,7}  go={2,7}   {3}{4}%" -f $case.Name, $z.QPS, $g.QPS, $mark, $d)
        }
    }
}
