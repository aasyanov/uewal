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
	b.Append(payload, key, meta)

	payload[0] = 'X'
	key[0] = 'Y'
	meta[0] = 'Z'

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
	b.AppendUnsafe(payload, key, meta)

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
	b.Append([]byte("a"), nil, nil)
	b.Append([]byte("b"), nil, nil)
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

func TestBatchAppend_WithKey(t *testing.T) {
	b := NewBatch(1)
	b.Append([]byte("p"), []byte("mykey"), nil)
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recKey := r.FieldByName("key").Bytes()
	if string(recKey) != "mykey" {
		t.Errorf("key=%q, want \"mykey\"", recKey)
	}
}

func TestBatchAppend_WithMeta(t *testing.T) {
	b := NewBatch(1)
	b.Append([]byte("p"), nil, []byte("mymeta"))
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recMeta := r.FieldByName("meta").Bytes()
	if string(recMeta) != "mymeta" {
		t.Errorf("meta=%q, want \"mymeta\"", recMeta)
	}
}

func TestEvent_WithTimestamp(t *testing.T) {
	ts := int64(1234567890)
	b := NewBatch(1)
	b.Append([]byte("p"), nil, nil, WithTimestamp(ts))
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
	b.Append([]byte("p"), nil, nil, WithNoCompress())
	v := reflect.ValueOf(b).Elem()
	noCompress := v.FieldByName("noCompress").Bool()
	if !noCompress {
		t.Errorf("WithNoCompress: noCompress=%v, want true", noCompress)
	}
}

func TestApplyOptions_DefaultTimestamp(t *testing.T) {
	b := NewBatch(1)
	b.Append([]byte("p"), nil, nil)
	v := reflect.ValueOf(b).Elem()
	records := v.FieldByName("records")
	r := records.Index(0)
	recTS := r.FieldByName("timestamp").Int()
	if recTS == 0 {
		t.Errorf("default timestamp: timestamp=0, want non-zero")
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
