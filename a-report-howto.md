# a-report-pprof.ps1 — How It Works

Automated profiling script for the `uewal` Go module.
Discovers all benchmarks, profiles each one, and produces a single
markdown report optimized for AI-assisted performance analysis.

## Pipeline

```
Phase 0: Clean profiles/
Phase 1: go test -list '^Benchmark' .  →  discover all bench functions
Phase 2: go test -c -o __bench_test.exe .  →  compile once
Phase 3: calibration (-benchtime=1x)  →  measure ns/op per benchmark
Phase 4: profiling  →  run each bench with adaptive benchtime + cpuprofile + memprofile
Phase 5: pprof top + parse metrics  →  write a-report-{timestamp}.md
Phase 6: remove __bench_test.exe
```

## Why calibration?

CPU profiler samples at 100 Hz. A benchmark running for 1 second collects
~100 samples — enough for top-5 hotspots. For 200 samples (the default
`-TargetSamples`) we need ~2 seconds.

But slow benchmarks (Replay_100K, Recovery_100K, E2E_*) may take 50-200ms
per iteration; running them for 5s would only add 1-2 more iterations with
no profile benefit. Fast benchmarks (CRC, CopyBytes, ApplyOptions) at 5-20 ns/op
will run millions of iterations regardless of benchtime.

**Calibration runs each benchmark once** (`-benchtime=1x`) and uses the measured
`ns/op` to compute the ideal benchtime: enough wall-clock seconds for
`TargetSamples` profiler samples, clamped to `[MinBenchSec, MaxBenchSec]`.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-BenchFilter` | `.` | Regex filter on benchmark names |
| `-MinBenchSec` | `1` | Floor for computed benchtime |
| `-MaxBenchSec` | `5` | Ceiling for computed benchtime |
| `-TargetSamples` | `200` | Target CPU profiler samples (100 Hz) |
| `-PprofTop` | `15` | Number of top functions per profile |
| `-SkipCalibration` | off | Use fixed benchtime instead |
| `-FixedBenchTime` | `2s` | Benchtime when calibration is skipped |
| `-NoClean` | off | Keep existing profiles/ contents |

## Outputs

- `a-report-{timestamp}.md` — consolidated report with summary table
  and per-benchmark CPU/memory pprof tops
- `profiles/cpu_{BenchmarkName}.prof` — raw CPU profiles (viewable in `go tool pprof`)
- `profiles/mem_{BenchmarkName}.prof` — raw memory profiles

## Usage Examples

```powershell
# All benchmarks, default settings (~3 min for ~120 benchmarks)
.\a-report-pprof.ps1

# Only Append-related benchmarks
.\a-report-pprof.ps1 -BenchFilter 'Append'

# More profiler accuracy, longer run
.\a-report-pprof.ps1 -TargetSamples 500 -MaxBenchSec 10

# Quick pass with fixed 1s benchtime, no calibration
.\a-report-pprof.ps1 -SkipCalibration -FixedBenchTime '1s'

# Keep old profiles, add only new ones
.\a-report-pprof.ps1 -NoClean -BenchFilter 'Replay'
```

## Report Structure

The generated markdown report contains:

1. **Header** — timestamp, Go version, OS, benchmark count, filter
2. **Summary table** — one row per benchmark: ns/op, B/op, allocs/op, MB/s, iterations, benchtime used
3. **Detailed Profiles** — per-benchmark section with:
   - Metrics table
   - CPU profile top (cumulative, `go tool pprof -top -cum`)
   - Memory profile top (alloc_objects cumulative)
4. **Errors** — benchmarks that failed to produce results
5. **Timing** — total/compilation/profiling durations

## Design Decisions

**Pre-compilation** (`go test -c`): compiling once saves ~2-5 minutes vs
108 separate `go test` invocations that each recompile.

**Root package only**: `internal/crc` benchmarks test the standard library's
CRC-32C — we can't optimize that code, so profiling it wastes time.
The root package re-exports CRC via `bench_test.go` as `BenchmarkCRC32C_*`
for comparison purposes; those are included.

**No `-show` filter on pprof**: full cumulative output is more useful for AI
analysis — it reveals which runtime/syscall functions dominate, which helps
identify whether bottlenecks are in user code or infrastructure.

**Adaptive benchtime**: avoids both under-profiling (too few samples for fast
ops) and over-waiting (minutes on slow ops that only need a few iterations).
