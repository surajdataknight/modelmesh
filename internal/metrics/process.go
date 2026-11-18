package metrics

import (
	"os"
	"runtime"
	rtmetrics "runtime/metrics"
	"strconv"
	"strings"
)

// RegisterRuntime adds Go runtime and (on Linux) process metrics.
func RegisterRuntime(r *Registry) {
	r.NewGaugeFunc("go_goroutines", "Number of goroutines that currently exist.", nil,
		func(emit func(float64, ...string)) { emit(float64(runtime.NumGoroutine())) })

	samples := []rtmetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/heap/allocs:bytes"},
	}
	read := func(i int) float64 {
		s := []rtmetrics.Sample{samples[i]}
		rtmetrics.Read(s)
		if s[0].Value.Kind() == rtmetrics.KindUint64 {
			return float64(s[0].Value.Uint64())
		}
		return 0
	}
	r.NewGaugeFunc("go_heap_objects_bytes", "Bytes occupied by live and unswept heap objects.", nil,
		func(emit func(float64, ...string)) { emit(read(0)) })
	r.NewCounterFunc("go_gc_cycles_total", "Completed GC cycles.", nil,
		func(emit func(float64, ...string)) { emit(read(1)) })
	r.NewCounterFunc("go_heap_allocs_bytes_total", "Cumulative bytes allocated on the heap.", nil,
		func(emit func(float64, ...string)) { emit(read(2)) })

	if _, err := os.Stat("/proc/self/stat"); err == nil {
		r.NewCounterFunc("process_cpu_seconds_total", "User and system CPU time spent, in seconds.", nil,
			func(emit func(float64, ...string)) {
				if cpu, _, ok := ProcStat("self"); ok {
					emit(cpu)
				}
			})
		r.NewGaugeFunc("process_resident_memory_bytes", "Resident set size, in bytes.", nil,
			func(emit func(float64, ...string)) {
				if _, rss, ok := ProcStat("self"); ok {
					emit(rss)
				}
			})
	}
}

// ProcStat reads CPU seconds and RSS bytes for a Linux pid ("self" allowed)
// from /proc. It assumes USER_HZ=100, which holds on mainstream Linux.
func ProcStat(pid string) (cpuSeconds, rssBytes float64, ok bool) {
	b, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		return 0, 0, false
	}
	// Fields after the parenthesized command name, which may contain spaces.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, 0, false
	}
	f := strings.Fields(s[i+1:])
	// f[0] is field 3 (state); utime=14, stime=15, rss=24 -> f[11], f[12], f[21].
	if len(f) < 22 {
		return 0, 0, false
	}
	ut, err1 := strconv.ParseFloat(f[11], 64)
	st, err2 := strconv.ParseFloat(f[12], 64)
	rss, err3 := strconv.ParseFloat(f[21], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, false
	}
	return (ut + st) / 100, rss * float64(os.Getpagesize()), true
}
