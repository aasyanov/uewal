package uewal

import (
	"reflect"
	"testing"
)

func TestNewBatch(t *testing.T) {
	b := NewBatch(5)
	if b.Len() != 0 {
		t.Errorf("NewBatch(5): Len()=%d, want 0", b.Len())
	}
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	if records.Kind() != reflect.Slice {
		t.Fatal("records is not a slice")
	}
	if cap := records.Cap(); cap != 5 {
		t.Errorf("NewBatch(5): capacity=%d, want 5", cap)
	}
}

func TestBatchAppend(t *testing.T) {
	payload := []byte("payload")
	key := []byte("key")
	meta := []byte("meta")
	b := NewBatch(4)
	b.Append(payload, WithKey(key), WithMeta(meta))

	// Modify originals after Append
	payload[0] = 'X'
	key[0] = 'Y'
	meta[0] = 'Z'

	// Batch should have copies; originals' modifications must not affect batch
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recPayload := r.FieldByName("payload").Bytes()
	recKey := r.FieldByName("key").Bytes()
	recMeta := r.FieldByName("meta").Bytes()

	if string(recPayload) != "payload" {
		t.Errorf("Append: payload in batch=%q, want \"payload\" (original was modified)", recPayload)
	}
	if string(recKey) != "key" {
		t.Errorf("Append: key in batch=%q, want \"key\" (original was modified)", recKey)
	}
	if string(recMeta) != "meta" {
		t.Errorf("Append: meta in batch=%q, want \"meta\" (original was modified)", recMeta)
	}
}

func TestEvent_BatchAppendUnsafe(t *testing.T) {
	payload := []byte("payload")
	key := []byte("key")
	meta := []byte("meta")
	b := NewBatch(4)
	b.AppendUnsafe(payload, WithKey(key), WithMeta(meta))

	// AppendUnsafe shares backing array; modifying original must affect batch
	payload[0] = 'X'
	key[0] = 'Y'
	meta[0] = 'Z'

	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recPayload := r.FieldByName("payload").Bytes()
	recKey := r.FieldByName("key").Bytes()
	recMeta := r.FieldByName("meta").Bytes()

	if string(recPayload) != "Xayload" {
		t.Errorf("AppendUnsafe: payload in batch=%q, want \"Xayload\" (should share backing array)", recPayload)
	}
	if string(recKey) != "Yey" {
		t.Errorf("AppendUnsafe: key in batch=%q, want \"Yey\" (should share backing array)", recKey)
	}
	if string(recMeta) != "Zeta" {
		t.Errorf("AppendUnsafe: meta in batch=%q, want \"Zeta\" (should share backing array)", recMeta)
	}
}

func TestBatchReset(t *testing.T) {
	b := NewBatch(3)
	b.Append([]byte("a"))
	b.Append([]byte("b"))
	if b.Len() != 2 {
		t.Fatalf("before Reset: Len()=%d, want 2", b.Len())
	}
	b.Reset()
	if b.Len() != 0 {
		t.Errorf("after Reset: Len()=%d, want 0", b.Len())
	}
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	if cap := records.Cap(); cap != 3 {
		t.Errorf("after Reset: capacity=%d, want 3 (capacity should be preserved)", cap)
	}
}

func TestWithKey(t *testing.T) {
	key := []byte("mykey")
	b := NewBatch(1)
	b.Append([]byte("p"), WithKey(key))
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recKey := r.FieldByName("key").Bytes()
	if string(recKey) != "mykey" {
		t.Errorf("WithKey: key=%q, want \"mykey\"", recKey)
	}
}

func TestWithMeta(t *testing.T) {
	meta := []byte("mymeta")
	b := NewBatch(1)
	b.Append([]byte("p"), WithMeta(meta))
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recMeta := r.FieldByName("meta").Bytes()
	if string(recMeta) != "mymeta" {
		t.Errorf("WithMeta: meta=%q, want \"mymeta\"", recMeta)
	}
}

func TestEvent_WithTimestamp(t *testing.T) {
	ts := int64(1234567890)
	b := NewBatch(1)
	b.Append([]byte("p"), WithTimestamp(ts))
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recTS := r.FieldByName("timestamp").Int()
	if recTS != ts {
		t.Errorf("WithTimestamp: timestamp=%d, want %d", recTS, ts)
	}
}

func TestWithNoCompress(t *testing.T) {
	b := NewBatch(1)
	b.Append([]byte("p"), WithNoCompress())
	v := reflect.ValueOf(b).Elem()
	noCompress := v.FieldByName("noCompress").Bool()
	if !noCompress {
		t.Errorf("WithNoCompress: noCompress=%v, want true", noCompress)
	}
}

func TestApplyOptions_DefaultTimestamp(t *testing.T) {
	b := NewBatch(1)
	b.Append([]byte("p")) // no WithTimestamp
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recTS := r.FieldByName("timestamp").Int()
	if recTS == 0 {
		t.Errorf("applyOptions without WithTimestamp: timestamp=0, want non-zero")
	}
}

func TestCopyBytes(t *testing.T) {
	if got := copyBytes(nil); got != nil {
		t.Errorf("copyBytes(nil)=%v, want nil", got)
	}
	if got := copyBytes([]byte{}); got != nil {
		t.Errorf("copyBytes([])=%v, want nil", got)
	}
	data := []byte("hello")
	got := copyBytes(data)
	if string(got) != "hello" {
		t.Errorf("copyBytes(%q)=%q, want \"hello\"", data, got)
	}
	// Must be a copy: modifying original must not affect result
	data[0] = 'X'
	if string(got) != "hello" {
		t.Errorf("after modifying original: copyBytes result=%q, want \"hello\" (must be independent copy)", got)
	}
}

func TestSliceOrNil(t *testing.T) {
	if got := sliceOrNil(nil); got != nil {
		t.Errorf("sliceOrNil(nil)=%v, want nil", got)
	}
	if got := sliceOrNil([]byte{}); got != nil {
		t.Errorf("sliceOrNil([])=%v, want nil", got)
	}
	data := []byte("x")
	got := sliceOrNil(data)
	if got == nil {
		t.Errorf("sliceOrNil(%q)=nil, want same slice", data)
	}
	if string(got) != "x" {
		t.Errorf("sliceOrNil(%q)=%q, want \"x\"", data, got)
	}
}
