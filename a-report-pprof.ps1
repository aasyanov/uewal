<#
.SYNOPSIS
    Automated benchmark profiling for AI-assisted optimization analysis.
    Produces per-benchmark CPU/memory profiles and a consolidated markdown report.

.DESCRIPTION
    See ai-report.llm.md for full documentation.

    1. Auto-discovers all Benchmark* functions via `go test -list`
    2. Pre-compiles the test binary once (saves ~2-5 min on repeated compilations)
    3. Runs a fast calibration pass (-benchtime=1x) to measure per-op cost
    4. Adapts benchtime per benchmark: fast ops get more time, slow ops less
    5. Collects CPU + memory profiles, parses metrics, runs pprof top
    6. Outputs timestamped ai-report-{ts}.md ready for AI consumption

.PARAMETER BenchFilter
    Regex filter for benchmark names. Default: '.' (all benchmarks).
    Example: -BenchFilter 'Append|Encode'

.PARAMETER MinBenchSec
    Minimum benchmark duration in seconds. Default: 1.

.PARAMETER MaxBenchSec
    Maximum benchmark duration in seconds. Default: 5.

.PARAMETER TargetSamples
    Target number of CPU profiler samples (100 Hz). Default: 200.
    Higher = more accurate profiles, longer runtime.

.PARAMETER PprofTop
    Number of top functions to include in pprof output. Default: 15.

.PARAMETER SkipCalibration
    Skip calibration pass and use fixed -benchtime for all. Default: false.

.PARAMETER FixedBenchTime
    Fixed benchtime when -SkipCalibration is set. Default: '2s'.

.PARAMETER Clean
    Remove all files from profiles/ before running. Default: true.

.EXAMPLE
    .\ai-report-pprof.ps1
    .\ai-report-pprof.ps1 -BenchFilter 'Append' -TargetSamples 300
    .\ai-report-pprof.ps1 -SkipCalibration -FixedBenchTime '1s'
#>
param(
    [string]$BenchFilter = '.',
    [double]$MinBenchSec = 1,
    [double]$MaxBenchSec = 5,
    [int]$TargetSamples = 200,
    [int]$PprofTop = 15,
    [switch]$SkipCalibration,
    [string]$FixedBenchTime = '2s',
    [switch]$NoClean
)

$ErrorActionPreference = "Continue"
$profileDir = ".\profiles"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportFile = "ai-report-${timestamp}.md"
$testBinary = ".\__bench_test.exe"
$modulePath = "github.com/aasyanov/uewal"

# --- Helpers -------------------------------------------------

function Write-Step([string]$msg) {
    Write-Host "`n=== $msg ===" -ForegroundColor Yellow
}

function Write-Info([string]$msg) {
    Write-Host "  $msg" -ForegroundColor Gray
}

function Write-Ok([string]$msg) {
    Write-Host "  [OK] $msg" -ForegroundColor Green
}

function Write-Err([string]$msg) {
    Write-Host "  [FAIL] $msg" -ForegroundColor Red
}

function Format-Duration([double]$seconds) {
    if ($seconds -ge 60) {
        $m = [math]::Floor($seconds / 60)
        $s = [math]::Round($seconds % 60)
        return "${m}m ${s}s"
    }
    return "$([math]::Round($seconds, 1))s"
}

function Compute-BenchTime([double]$nsPerOp) {
    # CPU profiler samples at 100 Hz. To get $TargetSamples samples,
    # we need $TargetSamples / 100 = N seconds of benchmark execution.
    # But Go auto-scales b.N to fill benchtime, so we just need enough wall time.
    $neededSec = $TargetSamples / 100.0

    # For very fast ops (< 100ns), the framework overhead dominates at low benchtime,
    # so we ensure at least MinBenchSec.
    # For slow ops (> 10ms), even 1s gives only ~100 iterations, which is fine for profiling.
    $sec = [math]::Max($MinBenchSec, [math]::Min($MaxBenchSec, $neededSec))

    # Slow ops: if one iteration takes > 50ms, cap at 2s to avoid extremely long runs
    if ($nsPerOp -gt 50000000) {
        $sec = [math]::Min($sec, 2)
    }

    return "$([math]::Ceiling($sec))s"
}

function Parse-BenchLine([string]$line) {
    if ($line -match '^(Benchmark\S+?)(?:-\d+)?\s+(\d+)\s+([0-9.]+)\s+ns/op(?:\s+([0-9.]+)\s+MB/s)?(?:\s+(\d+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?') {
        return [PSCustomObject]@{
            Name        = $matches[1]
            Iterations  = [long]$matches[2]
            NsPerOp     = [double]$matches[3]
            MBps        = if ($matches[4]) { [double]$matches[4] } else { $null }
            BytesPerOp  = if ($matches[5]) { [long]$matches[5] } else { $null }
            AllocsPerOp = if ($matches[6]) { [long]$matches[6] } else { $null }
        }
    }
    return $null
}

# --- Phase 0: Clean profiles/ --------------------------------

if (-not $NoClean) {
    Write-Step "Cleaning profiles/"
    if (Test-Path $profileDir) {
        Remove-Item "$profileDir\*" -Recurse -Force -ErrorAction SilentlyContinue
        Write-Ok "Cleared $(($profileDir))"
    }
}

if (-not (Test-Path $profileDir)) {
    New-Item -ItemType Directory -Path $profileDir | Out-Null
}

# --- Phase 1: Discover benchmarks ----------------------------

Write-Step "Discovering benchmarks"

$allBenchmarks = go test -list='^Benchmark' . 2>$null |
    Where-Object { $_ -match '^Benchmark' } |
    ForEach-Object { $_.Trim() }

if (-not $allBenchmarks -or $allBenchmarks.Count -eq 0) {
    Write-Err "No benchmarks found in root package."
    exit 1
}

# Apply user filter
$benchmarks = @($allBenchmarks | Where-Object { $_ -match $BenchFilter })
$totalAll = $allBenchmarks.Count
$total = $benchmarks.Count

Write-Ok "Found $totalAll benchmarks in root package, $total match filter '$BenchFilter'"

if ($total -eq 0) {
    Write-Err "No benchmarks match filter. Exiting."
    exit 1
}

# --- Phase 2: Pre-compile test binary ------------------------

Write-Step "Compiling test binary"
$compileStart = Get-Date

go test -c -o $testBinary . 2>&1 | Out-Host

if (-not (Test-Path $testBinary)) {
    Write-Err "Compilation failed. Exiting."
    exit 1
}

$compileSec = ((Get-Date) - $compileStart).TotalSeconds
Write-Ok "Compiled in $(Format-Duration $compileSec)"

# --- Phase 3: Calibration pass -------------------------------

$benchTimes = @{}

if (-not $SkipCalibration) {
    Write-Step "Calibration pass (-benchtime=1x)"
    Write-Info "Running each benchmark once to measure per-op cost..."

    $calStart = Get-Date
    $calI = 0

    foreach ($bench in $benchmarks) {
        $calI++
        Write-Host "`r  [$calI/$total] Calibrating $bench" -NoNewline -ForegroundColor DarkGray

        $args_ = @("-test.run=^$", "-test.bench=^${bench}$", "-test.benchtime=1x", "-test.count=1", "-test.benchmem", "-test.timeout=60s")
        $output = & $testBinary @args_ 2>&1

        $resultLine = $output | Select-String -Pattern "^$bench" | Select-Object -Last 1
        $parsed = Parse-BenchLine "$($resultLine)"

        if ($parsed) {
            $bt = Compute-BenchTime $parsed.NsPerOp
            $benchTimes[$bench] = $bt
        } else {
            $benchTimes[$bench] = "${MinBenchSec}s"
        }
    }

    Write-Host ""
    $calSec = ((Get-Date) - $calStart).TotalSeconds
    Write-Ok "Calibration done in $(Format-Duration $calSec)"

    # Show distribution
    $dist = $benchTimes.Values | Group-Object | Sort-Object Name |
        ForEach-Object { "  $($_.Name): $($_.Count) benchmarks" }
    Write-Info "Benchtime distribution:"
    $dist | ForEach-Object { Write-Info $_ }
} else {
    Write-Info "Skipping calibration, using fixed -benchtime=$FixedBenchTime"
    foreach ($bench in $benchmarks) {
        $benchTimes[$bench] = $FixedBenchTime
    }
}

# --- Phase 4: Profiling pass ---------------------------------

Write-Step "Profiling pass ($total benchmarks)"

$results = @{}
$errors = @()
$profileStart = Get-Date
$i = 0

foreach ($bench in $benchmarks) {
    $i++
    $bt = $benchTimes[$bench]
    $cpuFile = "$profileDir\cpu_$bench.prof"
    $memFile = "$profileDir\mem_$bench.prof"

    Write-Host "[$i/$total] $bench " -NoNewline -ForegroundColor Cyan
    Write-Host "(${bt})" -ForegroundColor DarkCyan

    $args_ = @("-test.run=^$", "-test.bench=^${bench}$", "-test.benchtime=$bt", "-test.count=1", "-test.cpuprofile=$cpuFile", "-test.memprofile=$memFile", "-test.benchmem", "-test.timeout=120s")
    $output = & $testBinary @args_ 2>&1

    $resultLine = $output | Select-String -Pattern "^$bench" | Select-Object -Last 1
    $parsed = Parse-BenchLine "$($resultLine)"

    if ($parsed) {
        $results[$bench] = $parsed

        # Short inline summary
        $summary = "$($parsed.Iterations) iters, $($parsed.NsPerOp) ns/op"
        if ($parsed.BytesPerOp -ne $null) { $summary += ", $($parsed.BytesPerOp) B/op" }
        if ($parsed.AllocsPerOp -ne $null) { $summary += ", $($parsed.AllocsPerOp) allocs/op" }
        Write-Ok $summary
    } else {
        Write-Err "Failed to parse result"
        $errors += $bench
        $errOutput = ($output | Out-String).Trim()
        if ($errOutput) {
            Write-Info "Output: $errOutput"
        }
    }
}

$profileSec = ((Get-Date) - $profileStart).TotalSeconds
Write-Ok "Profiling done in $(Format-Duration $profileSec)"

# --- Phase 5: Generate report --------------------------------

Write-Step "Generating report: $reportFile"

$report = [System.Text.StringBuilder]::new()

[void]$report.AppendLine("# UEWAL Benchmark Profiling Report")
[void]$report.AppendLine("")
[void]$report.AppendLine("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
[void]$report.AppendLine("Module: ``$modulePath``")
[void]$report.AppendLine("Go version: $(go version)")
[void]$report.AppendLine("OS: $([System.Runtime.InteropServices.RuntimeInformation]::OSDescription)")
[void]$report.AppendLine("Benchmarks: $total (of $totalAll total)")
[void]$report.AppendLine("Filter: ``$BenchFilter``")
[void]$report.AppendLine("Target CPU samples: $TargetSamples (at 100 Hz)")
[void]$report.AppendLine("")

# Summary table
[void]$report.AppendLine("## Summary")
[void]$report.AppendLine("")
[void]$report.AppendLine("| Benchmark | ns/op | B/op | allocs/op | MB/s | iters | benchtime |")
[void]$report.AppendLine("|-----------|------:|-----:|----------:|-----:|------:|-----------|")

foreach ($bench in $benchmarks) {
    if ($results.ContainsKey($bench)) {
        $r = $results[$bench]
        $mbps = if ($r.MBps -ne $null) { "$($r.MBps)" } else { "-" }
        $bop = if ($r.BytesPerOp -ne $null) { "$($r.BytesPerOp)" } else { "-" }
        $aop = if ($r.AllocsPerOp -ne $null) { "$($r.AllocsPerOp)" } else { "-" }
        $bt = $benchTimes[$bench]
        [void]$report.AppendLine("| $bench | $($r.NsPerOp) | $bop | $aop | $mbps | $($r.Iterations) | $bt |")
    } else {
        [void]$report.AppendLine("| $bench | ERROR | - | - | - | - | - |")
    }
}
[void]$report.AppendLine("")

# Per-benchmark details with pprof
[void]$report.AppendLine("## Detailed Profiles")
[void]$report.AppendLine("")

foreach ($bench in $benchmarks) {
    [void]$report.AppendLine("### $bench")
    [void]$report.AppendLine("")

    if ($results.ContainsKey($bench)) {
        $r = $results[$bench]
        [void]$report.AppendLine("| Metric | Value |")
        [void]$report.AppendLine("|--------|------:|")
        [void]$report.AppendLine("| ns/op | $($r.NsPerOp) |")
        [void]$report.AppendLine("| iterations | $($r.Iterations) |")
        if ($r.MBps -ne $null) { [void]$report.AppendLine("| MB/s | $($r.MBps) |") }
        if ($r.BytesPerOp -ne $null) { [void]$report.AppendLine("| B/op | $($r.BytesPerOp) |") }
        if ($r.AllocsPerOp -ne $null) { [void]$report.AppendLine("| allocs/op | $($r.AllocsPerOp) |") }
        [void]$report.AppendLine("| benchtime | $($benchTimes[$bench]) |")
        [void]$report.AppendLine("")
    }

    # CPU profile
    $cpuFile = "$profileDir\cpu_$bench.prof"
    if (Test-Path $cpuFile) {
        $cpuTop = & go tool pprof -top "-nodecount=$PprofTop" -cum "$cpuFile" 2>$null | Out-String
        if ($cpuTop.Trim()) {
            [void]$report.AppendLine("**CPU profile** (top $PprofTop, cumulative)")
            [void]$report.AppendLine('```')
            [void]$report.AppendLine($cpuTop.Trim())
            [void]$report.AppendLine('```')
            [void]$report.AppendLine("")
        }
    }

    # Memory profile
    $memFile = "$profileDir\mem_$bench.prof"
    if (Test-Path $memFile) {
        $memTop = & go tool pprof -top "-nodecount=$PprofTop" -alloc_objects -cum "$memFile" 2>$null | Out-String
        if ($memTop.Trim()) {
            [void]$report.AppendLine("**Memory profile** (top $PprofTop, alloc_objects cumulative)")
            [void]$report.AppendLine('```')
            [void]$report.AppendLine($memTop.Trim())
            [void]$report.AppendLine('```')
            [void]$report.AppendLine("")
        }
    }

    [void]$report.AppendLine("---")
    [void]$report.AppendLine("")
}

# Errors section
if ($errors.Count -gt 0) {
    [void]$report.AppendLine("## Errors")
    [void]$report.AppendLine("")
    foreach ($e in $errors) {
        [void]$report.AppendLine("- ``$e``: failed to produce results")
    }
    [void]$report.AppendLine("")
}

# Timing
$totalSec = ((Get-Date) - $compileStart).TotalSeconds + $compileSec
[void]$report.AppendLine("---")
[void]$report.AppendLine("*Total runtime: $(Format-Duration $totalSec) | Compilation: $(Format-Duration $compileSec) | Profiling: $(Format-Duration $profileSec)*")

$report.ToString() | Out-File -FilePath $reportFile -Encoding UTF8

Write-Ok "Report written to $reportFile"

# --- Phase 6: Cleanup ----------------------------------------

if (Test-Path $testBinary) {
    Remove-Item $testBinary -Force
    Write-Info "Removed $testBinary"
}

# --- Done ----------------------------------------------------

$totalElapsed = ((Get-Date) - $compileStart).TotalSeconds + $compileSec
Write-Step "Done"
Write-Host "  Report:     $reportFile" -ForegroundColor Green
Write-Host "  Profiles:   $profileDir\" -ForegroundColor Green
Write-Host "  Benchmarks: $total" -ForegroundColor Green
Write-Host "  Errors:     $($errors.Count)" -ForegroundColor $(if ($errors.Count -gt 0) { "Red" } else { "Green" })
Write-Host "  Total time: $(Format-Duration $totalElapsed)" -ForegroundColor Green
Write-Host ""
