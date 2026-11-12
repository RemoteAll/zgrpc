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
$minTests = 67

# 随机空闲端口：Windows 上 `Stop-Process -Force`（= TerminateProcess）会让监听 socket
# 变成 zombie LISTEN（netstat 里 OwningProcess=0），持续数十秒且无法 kill。
# 固定在 18051/18052 会让下一次回归全部 SKIP，所以每段用独立随机端口。
function Get-FreePort {
    for ($i = 0; $i -lt 40; $i++) {
        $p = Get-Random -Minimum 18200 -Maximum 18999
        if (-not (Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue)) { return $p }
    }
    return (Get-Random -Minimum 18200 -Maximum 18999)
}
$smokePort = Get-FreePort
$reflPort = Get-FreePort
$rustPort = Get-FreePort
$zigSrvPort = Get-FreePort

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

# ---------- 2.5 代码生成（proto3 子集）：生成 + 语法检查 + 生成代码单测 ----------
$genLog = Join-Path $reportDir 'codegen.log'
$genOut = & $zig build codegen 2>&1 | Out-String
$genCode = $LASTEXITCODE
Add-Content -Path $genLog -Value $genOut -Encoding UTF8
if ($genCode -ne 0) {
    Write-Report "[回归] FAIL：zig build codegen exit=$genCode（详见 $genLog）"
    $failed += 'codegen'
} else {
    $astOut = & $zig ast-check 'examples\echo.pb.zig' 2>&1 | Out-String
    $astCode = $LASTEXITCODE
    Add-Content -Path $genLog -Value $astOut -Encoding UTF8
    if ($astCode -ne 0) {
        Write-Report "[回归] FAIL：生成的 echo.pb.zig 语法检查未通过（详见 $genLog）"
        $failed += 'codegen-ast'
    } else {
        # 生成代码的编解码单测（examples/echo_pb_test.zig，独立一套）
        $pbOut = & $zig build test 2>&1 | Out-String
        $pbCode = $LASTEXITCODE
        Add-Content -Path $genLog -Value $pbOut -Encoding UTF8
        if ($pbCode -ne 0) {
            Write-Report "[回归] FAIL：zig build test（含生成代码单测）exit=$pbCode（详见 $genLog）"
            $failed += 'codegen-test'
        } else {
            Write-Report "[回归] PASS：codegen（生成 + ast-check + 生成代码单测）"
        }
    }
}

# ---------- 2.6 客户端本地读超时：服务端只接连接、永不回包 ----------
if ($SkipSmoke) {
    Write-Report "[回归] SKIP：客户端超时探针（-SkipSmoke）"
} else {
    $probeExe = Join-Path $root 'zig-out\bin\timeout-probe.exe'
    if (-not (Test-Path $probeExe)) {
        Write-Report "[回归] SKIP：客户端超时探针（缺 timeout-probe.exe，先跑一次不带 -SkipBuild 的回归）"
    } else {
        $po = & $probeExe 2>&1 | Out-String
        $pc = $LASTEXITCODE
        Add-Content -Path $report -Value "[回归] timeout-probe：$po" -Encoding UTF8
        if ($pc -eq 0) {
            Write-Report "[回归] PASS：客户端本地读超时（服务端不响应时按设定时间退出）"
        } else {
            Write-Report "[回归] FAIL：客户端本地读超时 exit=$pc（详见 $report）"
            $failed += 'timeout-probe'
        }
    }
}

# ---------- 2.7 Windows AFD 原生读超时（overlapped + NtWaitForSingleObject） ----------
if ($SkipSmoke) {
    Write-Report "[回归] SKIP：AFD 原生超时探针（-SkipSmoke）"
} else {
    $afdExe = Join-Path $root 'zig-out\bin\afd-probe.exe'
    if (-not (Test-Path $afdExe)) {
        Write-Report "[回归] SKIP：AFD 原生超时探针（缺 afd-probe.exe，先跑一次不带 -SkipBuild 的回归）"
    } else {
        $ao = & $afdExe 2>&1 | Out-String
        $ac = $LASTEXITCODE
        Add-Content -Path $report -Value "[回归] afd-probe：$ao" -Encoding UTF8
        if ($ac -eq 0) {
            Write-Report "[回归] PASS：AFD 原生读超时（内核层产生 Timeout，零线程）"
        } else {
            Write-Report "[回归] FAIL：AFD 原生读超时 exit=$ac（详见 $report）"
            $failed += 'afd-probe'
        }
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
        $ports = Get-NetTCPConnection -LocalPort $smokePort -State Listen -ErrorAction SilentlyContinue
        if ($ports) {
            Write-Report "[回归] SKIP：冒烟（$smokePort 已被占用，pid=$(($ports | Select-Object -First 1).OwningProcess)）"
        } else {
            $serverOut = "$reportDir\server.log"
            $server = Start-Process -FilePath $serverExe -ArgumentList @('--port', $smokePort.ToString()) -RedirectStandardError $serverOut -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 700
            try {
                $clientOut = & $clientExe $smokePort 2>&1 | Out-String
                $clientCode = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] 客户端输出：$clientOut" -Encoding UTF8
                if ($clientCode -eq 0) {
                    Write-Report "[回归] PASS：冒烟（并发双流 + bidi 3/3 + 健康检查 SERVING + grpc-timeout→DEADLINE_EXCEEDED + -bin metadata 往返 + grpc-web-text 往返 + pb 结构化往返）"
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

# ---------- 3.5 服务反射：grpcurl 无 .proto 自省 + 直接调用 ----------
$grpcurlExe = Join-Path $env:USERPROFILE 'go\bin\grpcurl.exe'
if ($SkipSmoke) {
    Write-Report "[回归] SKIP：反射（-SkipSmoke）"
} elseif (-not (Test-Path $grpcurlExe)) {
    Write-Report "[回归] SKIP：反射（未找到 grpcurl；go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest）"
} elseif (-not (Test-Path (Join-Path $root 'zig-out\bin\echo-server.exe'))) {
    Write-Report "[回归] SKIP：反射（缺少 echo-server 产物）"
} else {
    $ports = Get-NetTCPConnection -LocalPort $reflPort -State Listen -ErrorAction SilentlyContinue
    if ($ports) {
        Write-Report "[回归] SKIP：反射（$reflPort 已被占用）"
    } else {
        $srv = Start-Process -FilePath (Join-Path $root 'zig-out\bin\echo-server.exe') -ArgumentList @('--port', $reflPort.ToString()) -RedirectStandardError "$reportDir\refl-server.log" -PassThru -WindowStyle Hidden
        Start-Sleep -Milliseconds 700
        try {
            $reflOk = $true
            $listOut = & $grpcurlExe -plaintext "127.0.0.1:$reflPort" list 2>&1 | Out-String
            Add-Content -Path $report -Value "[回归] grpcurl list：$listOut" -Encoding UTF8
            if ($listOut -notmatch 'echo\.Echo') { $reflOk = $false; Write-Report "[回归] FAIL：反射 list 未列出 echo.Echo" }

            $descOut = & $grpcurlExe -plaintext "127.0.0.1:$reflPort" describe echo.Echo 2>&1 | Out-String
            Add-Content -Path $report -Value "[回归] grpcurl describe：$descOut" -Encoding UTF8
            if ($descOut -notmatch 'rpc Say') { $reflOk = $false; Write-Report "[回归] FAIL：反射 describe 未解出 rpc Say" }

            $msgOut = & $grpcurlExe -plaintext "127.0.0.1:$reflPort" describe echo.Summary 2>&1 | Out-String
            Add-Content -Path $report -Value "[回归] grpcurl describe Summary：$msgOut" -Encoding UTF8
            if ($msgOut -notmatch 'echo\.Status') { $reflOk = $false; Write-Report "[回归] FAIL：反射 describe 未正确解出 enum 字段类型 echo.Status" }

            # 注意：PowerShell 5.1 调用原生 exe 时会吞掉内层双引号，JSON 必须走 stdin（-d @）
            $callOut = '{"value":3,"tag":"reflection-works"}' | & $grpcurlExe -plaintext -d '@' "127.0.0.1:$reflPort" echo.Echo/Say 2>&1 | Out-String
            Add-Content -Path $report -Value "[回归] grpcurl Say：$callOut" -Encoding UTF8
            if ($callOut -notmatch 'reflection-works') { $reflOk = $false; Write-Report "[回归] FAIL：反射后无 .proto 调用 Say 失败" }

            if ($reflOk) { Write-Report "[回归] PASS：反射（list + describe service/message + 无 .proto 调用）" }
            else { $failed += 'reflection' }
        } finally {
            if ($srv -and -not $srv.HasExited) { Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue }
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
        $p18061 = Get-NetTCPConnection -LocalPort $rustPort -State Listen -ErrorAction SilentlyContinue
        if ($p18061) {
            Write-Report "[回归] SKIP：对拍(a)（$rustPort 被占用）"
        } else {
            $rs = Start-Process -FilePath $interopExe -ArgumentList @('server', '--port', $rustPort.ToString()) -RedirectStandardError "$reportDir\rust-server.log" -RedirectStandardOutput "$reportDir\rust-server.out" -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 900
            try {
                $zo = & $clientExe2 $rustPort 2>&1 | Out-String
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
        $p18051 = Get-NetTCPConnection -LocalPort $zigSrvPort -State Listen -ErrorAction SilentlyContinue
        if ($p18051) {
            Write-Report "[回归] SKIP：对拍(b)(c)（$zigSrvPort 被占用，pid=$(($p18051 | Select-Object -First 1).OwningProcess)）"
        } else {
            $zs = Start-Process -FilePath $serverExe2 -ArgumentList @('--port', $zigSrvPort.ToString()) -RedirectStandardError "$reportDir\zig-server.log" -PassThru -WindowStyle Hidden
            Start-Sleep -Milliseconds 700
            try {
                $ro = & $interopExe client --port $zigSrvPort 2>&1 | Out-String
                $rc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] Rust client(原生) -> zgrpc server：$ro" -Encoding UTF8
                if ($rc -eq 0) {
                    Write-Report "[回归] PASS：对拍 Rust client(原生) -> zgrpc server"
                } else {
                    Write-Report "[回归] FAIL：对拍 Rust client(原生) exit=$rc"
                    $failed += 'interop-rust-native'
                }

                $rwo = & $interopExe client --port $zigSrvPort --web 2>&1 | Out-String
                $rwc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] Rust client(gRPC-Web) -> zgrpc server：$rwo" -Encoding UTF8
                if ($rwc -eq 0) {
                    Write-Report "[回归] PASS：对拍 Rust client(gRPC-Web) -> zgrpc server"
                } else {
                    Write-Report "[回归] FAIL：对拍 Rust client(gRPC-Web) exit=$rwc"
                    $failed += 'interop-rust-web'
                }

                $rto = & $interopExe client --port $zigSrvPort --web-text 2>&1 | Out-String
                $rtc = $LASTEXITCODE
                Add-Content -Path $report -Value "[回归] Rust client(gRPC-Web-text) -> zgrpc server：$rto" -Encoding UTF8
                if ($rtc -eq 0) {
                    Write-Report "[回归] PASS：对拍 Rust client(gRPC-Web-text) -> zgrpc server"
                } else {
                    Write-Report "[回归] FAIL：对拍 Rust client(gRPC-Web-text) exit=$rtc"
                    $failed += 'interop-rust-webtext'
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
