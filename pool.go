package uewal

import "sync"

// recordSlicePool amortizes per-Write allocation of []record.
var recordSlicePool = sync.Pool{
	New: func() any {
		s := make([]record, 0, 128)
		return &s
	},
}

func getRecordSlice(n int) ([]record, *[]record) {
	sp := recordSlicePool.Get().(*[]record)
	s := *sp
	if cap(s) < n {
		s = make([]record, n)
	} else {
		s = s[:n]
	}
	return s, sp
}

func putRecordSlice(sp *[]record, s []record) {
	clear(s)
	if cap(s) <= 1024 {
		*sp = s[:0]
		recordSlicePool.Put(sp)
	}
}
