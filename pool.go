package uewal

import "sync"

// recordSlicePool amortizes per-Append allocation of []record.
var recordSlicePool = sync.Pool{
	New: func() any {
		s := make([]record, 0, 128)
		return &s
	},
}

// payloadPools provides tiered reuse of byte buffers for common payload sizes.
// Class index 0..5 maps to sizes 64, 128, 256, 512, 1024, 4096.
var payloadPoolSizes = [...]int{64, 128, 256, 512, 1024, 4096}

var payloadPools [6]sync.Pool

func init() {
	for i := range payloadPools {
		sz := payloadPoolSizes[i]
		payloadPools[i].New = func() any {
			b := make([]byte, sz)
			return &b
		}
	}
}

// getPayloadBuf returns a buffer of at least size bytes from the tiered pool.
// Returns the buffer, pool class (1-based), or (nil, 0) if size exceeds all pools.
func getPayloadBuf(size int) ([]byte, int8) {
	for i, psz := range payloadPoolSizes {
		if size <= psz {
			bp := payloadPools[i].Get().(*[]byte)
			return (*bp)[:size], int8(i + 1)
		}
	}
	return make([]byte, size), 0
}

// putPayloadBuf returns a buffer to the appropriate pool.
func putPayloadBuf(buf []byte, class int8) {
	if class <= 0 || int(class) > len(payloadPools) {
		return
	}
	b := buf[:cap(buf)]
	payloadPools[class-1].Put(&b)
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
	for i := range s {
		if s[i].poolClass > 0 {
			putPayloadBuf(s[i].payload, s[i].poolClass)
		}
		s[i] = record{}
	}
	*sp = s[:0]
	recordSlicePool.Put(sp)
}
