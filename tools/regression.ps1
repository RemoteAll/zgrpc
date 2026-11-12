# zgrpc 一键回归：构建 -> 单测 -> 真实 socket 冒烟（并发双流）
# 退出码：0 = 全绿；1 = 有失败。报告落 artifacts\regression\report-<时间戳>.txt
#
# 用法：powershell -NoProfile -ExecutionPolicy Bypass -File tools\regression.ps1 [-SkipBuild] [-SkipSmoke]

param(
    [switch]$SkipBuild,
    [switch]$SkipSmoke,
    [switch]$SkipInterop
)

# 外部命令（zig 等）写 stderr 不能被当成终止错误，否则脚本会被炸掉且无提示
$ErrorActionPreference = 'Continue'

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# 测试数下限：只能抬高，不能降低（降低必须在 docs/决策记录.md 写明依据）
$minTests = 34

$reportDir = Join-Path $root 'artifacts\regression'
New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
$report = Join-Path $reportDir ('report-{0}.txt' -f (Get-Date -Format 'yyyyMMdd-HHmmss'))

function Write-Report([string]$msg) {
    Write-Host $msg
    Add-Content -Path $report -Value $msg -Encoding UTF8
}

# 定位 zig：优先显式路径，其次 PATH
$zig = 'E:\Soft\zig-x86_64-windows-0.17.1\zig.exe'
if (-not (Test-Path $zig)) {
    $cmd = Get-Command zig -ErrorAction SilentlyContinue
    if ($cmd) { $zig = $cmd.Source } else { $zig = '' }
}

Write-Report "[回归] zgrpc · $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Report "[回归] 项目根：$root"
if (-not $zig) {
    Write-Report "[回归] FAIL：找不到 zig 可执行文件（请设置 zig 到 PATH）"
    exit 1
}
Write-Report "[回归] zig：$zig"

$failed = @()

# ---------- 1. 构建 ----------
if ($SkipBuild) {
    Write-Report "[回归] SKIP：构建（-SkipBuild）"
} else {
    & $zig build *> "$reportDir\build.log"
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        Write-Report "[回归] FAIL：zig build 退出码 $code（详见 $reportDir\build.log）"
        $failed += 'build'
    } else {
        Write-Report "[回归] PASS：zig build"
    }
}

# ---------- 2. 单元/端到端测试（直接用 zig test：不吃构建缓存，每次真跑）----------
$testLog = "$reportDir\test.log"
& $zig test 'src\zgrpc.zig' *> $testLog
$testCode = $LASTEXITCODE
$testText = Get-Content $testLog -Raw -ErrorAction SilentlyContinue

if ($testCode -ne 0) {
    Write-Report "[回归] FAIL：zig test 退出码 $testCode（详见 $testLog）"
    $failed += 'test'
} else {
    $passed = -1
    $mAll = [regex]::Match($testText, 'All (\d+) tests passed')
    if ($mAll.Success) {
        $passed = [int]$mAll.Groups[1].Value
    } else {
        $m = [regex]::Match($testText, '(\d+)/(\d+) tests passed')
        if ($m.Success) { $passed = [int]$m.Groups[1].Value }
    }
    if ($passed -lt 0) {
        Write-Report "[回归] FAIL：无法从输出解析测试数（详见 $testLog）"
        $failed += 'test-parse'
    } elseif ($passed -lt $minTests) {
        Write-Report "[回归] FAIL：测试数 $passed 低于下限 $minTests（阈值只能抬高）"
        $failed += 'test-threshold'
    } else {
        Write-Report "[回归] PASS：tests $passed（下限 $minTests）"
    }
}

# ---------- 3. 冒烟：真实 socket 并发双流 ----------
if ($SkipSmoke) {
    Write-Report "[回归] SKIP：冒烟（-SkipSmoke）"
} else {
    $serverExe = Join-Path $root 'zig-out\bin\echo-server.exe'
    $clientExe = Join-Path $root 'zig-out\bin\echo-client.exe'
    if (-not (Test-Path $serverExe) -or -not (Test-Path $clientExe)) {
        Write-Report "[回归] SKIP：冒烟（缺少 echo-server/echo-client 产物，先跑一次不带 -SkipBuild 的回归）"
    } else {
        $ports = Get-NetTCPConnection -LocalPort 18051 -ErrorAction SilentlyContinue
        if ($ports) {
            Write-Report "[回归] SKIP：冒烟（18051 已被占用，pid=$(($ports | Select-Object -First 1).OwningProcess)）"
        } else {
            $serverOut = "$reportDir\server.log"
            $server = Start-Process -FilePath $serverExe -RedirectStandardError $serverOut -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 700
            try {
                $clientOut = & $clientExe 2>&1 | Out-String
                $clientCode = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] 客户端输出：$clientOut" -Encoding UTF8
                if ($clientCode -eq 0) {
                    Write-Report "[回归] PASS：冒烟（并发双流 + bidi 3/3 + 健康检查 SERVING + grpc-timeout→DEADLINE_EXCEEDED + -bin metadata 往返）"
                } else {
                    Write-Report "[回归] FAIL：冒烟 exit=$clientCode（详见 $report）"
                    $failed += 'smoke'
                }
            } finally {
                if ($server -and -not $server.HasExited) { Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue }
            }
        }
    }
}

# ---------- 4. 互操作对拍（Rust hyper 当裁判，双向）----------
if ($SkipInterop) {
    Write-Report "[回归] SKIP：互操作对拍（-SkipInterop）"
} else {
    $interopExe = Join-Path $root 'tools\interop-rust\target\release\zgrpc-interop.exe'
    $serverExe2 = Join-Path $root 'zig-out\bin\echo-server.exe'
    $clientExe2 = Join-Path $root 'zig-out\bin\echo-client.exe'
    if (-not (Test-Path $interopExe)) {
        Write-Report "[回归] SKIP：互操作对拍（缺 tools/interop-rust/target/release/zgrpc-interop.exe，先 cargo build --release）"
    } elseif (-not (Test-Path $serverExe2) -or -not (Test-Path $clientExe2)) {
        Write-Report "[回归] SKIP：互操作对拍（缺 zig 示例产物）"
    } else {
        # (a) Rust 服务端 -> zgrpc 客户端
        $p18061 = Get-NetTCPConnection -LocalPort 18061 -ErrorAction SilentlyContinue
        if ($p18061) {
            Write-Report "[回归] SKIP：对拍(a)（18061 被占用）"
        } else {
            $rs = Start-Process -FilePath $interopExe -ArgumentList @('server', '--port', '18061') -RedirectStandardError "$reportDir\rust-server.log" -RedirectStandardOutput "$reportDir\rust-server.out" -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 900
            try {
                $zo = & $clientExe2 18061 2>&1 | Out-String
                $zc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] zgrpc client -> Rust server：$zo" -Encoding UTF8
                if ($zc -eq 0) {
                    Write-Report "[回归] PASS：对拍 zgrpc client -> Rust server"
                } else {
                    Write-Report "[回归] FAIL：对拍 zgrpc client -> Rust server exit=$zc"
                    $failed += 'interop-zig-client'
                }
            } finally {
                if ($rs -and -not $rs.HasExited) { Stop-Process -Id $rs.Id -Force -ErrorAction SilentlyContinue }
            }
        }

        # (b)(c) zgrpc 服务端 -> Rust 客户端（原生 + gRPC-Web）
        $p18051 = Get-NetTCPConnection -LocalPort 18051 -ErrorAction SilentlyContinue
        if ($p18051) {
            Write-Report "[回归] SKIP：对拍(b)(c)（18051 被占用，pid=$(($p18051 | Select-Object -First 1).OwningProcess)）"
        } else {
            $zs = Start-Process -FilePath $serverExe2 -RedirectStandardError "$reportDir\zig-server.log" -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 700
            try {
                $ro = & $interopExe client --port 18051 2>&1 | Out-String
                $rc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] Rust client(原生) -> zgrpc server：$ro" -Encoding UTF8
                if ($rc -eq 0) {
                    Write-Report "[回归] PASS：对拍 Rust client(原生) -> zgrpc server"
                } else {
                    Write-Report "[回归] FAIL：对拍 Rust client(原生) exit=$rc"
                    $failed += 'interop-rust-native'
                }

                $rwo = & $interopExe client --port 18051 --web 2>&1 | Out-String
                $rwc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] Rust client(gRPC-Web) -> zgrpc server：$rwo" -Encoding UTF8
                if ($rwc -eq 0) {
                    Write-Report "[回归] PASS：对拍 Rust client(gRPC-Web) -> zgrpc server"
                } else {
                    Write-Report "[回归] FAIL：对拍 Rust client(gRPC-Web) exit=$rwc"
                    $failed += 'interop-rust-web'
                }
            } finally {
                if ($zs -and -not $zs.HasExited) { Stop-Process -Id $zs.Id -Force -ErrorAction SilentlyContinue }
            }
        }
    }
}

# ---------- 汇总 ----------
if ($failed.Count -eq 0) {
    Write-Report "[回归] 结果：PASS（0 失败）"
    exit 0
} else {
    Write-Report "[回归] 结果：FAIL -> $($failed -join ', ')"
    exit 1
}
