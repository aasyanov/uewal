# UEWAL — Technical Specification

Тотальная модификация uewal из single-file WAL в сегментированный commit log engine.
Один экспортируемый объект `WAL`. Обратная совместимость не сохраняется.

---

# 1. Цели

Строительный блок для event sourcing, message broker, embedded DB WAL,
replication log, CDC pipeline, durable queue.

- always segmented (no single-file mode)
- batch-first (amortized syscall + fsync)
- zero-copy replay (mmap, slice pointing)
- zero-allocation append (sync.Pool, atomic LSN)
- crash-safe (batch CRC, truncate recovery, manifest)
- extensible (Indexer, Compressor, Hooks)
- minimal wire footprint (no wasted bytes)

---

# 2. Архитектура

Три внутренних слоя. Один экспортируемый объект.

```
user code ──► WAL (public API)
               ├── segment manager   ротация, retention, manifest, snapshot
               ├── segment writer    append pipeline, encoding, fsync
               └── replay engine     mmap, iterator, sparse index
```

---

# 3. Wire Format — Batch Frame v1

Little-endian. CRC-32C (Castagnoli, hardware SSE4.2 / ARM CRC).
Ноль padding. Каждый байт несёт данные или флаги.

## 3.1 BatchHeader (28 bytes)

```
Offset  Field        Size  Notes
0       Magic        4B    "EWAL" — frame detection
4       Version      1B    =1 — wire format version
5       Flags        1B    feature flags (8 bits)
6       RecordCount  2B    max 65535 records per batch
8       FirstLSN     8B    first record LSN              ← 8B aligned
16      Timestamp    8B    batch timestamp (UnixNano)     ← 8B aligned
24      BatchSize    4B    total frame size incl. CRC     ← 4B aligned
```

Все 8B-поля на 8B-границах. Ноль padding.

### Flags (1 byte = 8 bits)

| Bit | Name | Описание |
|---|---|---|
| 0 | `compressed` | records region сжат (Compressor) |
| 1 | `per_record_ts` | каждый record содержит индивидуальный Timestamp 8B |
| 2-7 | reserved | = 0. Позволяет добавить 6 features без bump версии |

### Version strategy

- Version = 1 для первого релиза
- Bump только при несовместимых изменениях wire format
- Reader обязан поддерживать все Version <= current
- Новые features добавляются через Flags без bump Version
- Запас: 6 свободных flag bits + 254 свободных версии

## 3.2 Record (variable)

**Без индивидуального timestamp** (Flags bit 1 = 0):

```
Field        Size  Notes
KeyLen       2B    0 = no key
MetaLen      2B    0 = no meta
PayloadLen   4B
Key          KeyLen bytes
Meta         MetaLen bytes
Payload      PayloadLen bytes
= 8B overhead + variable data
```

**С индивидуальным timestamp** (Flags bit 1 = 1):

```
Field        Size  Notes
Timestamp    8B    per-record UnixNano
KeyLen       2B
MetaLen      2B
PayloadLen   4B
Key          KeyLen bytes
Meta         MetaLen bytes
Payload      PayloadLen bytes
= 16B overhead + variable data
```

### Логика выбора timestamp mode

Encoder автоматически решает:
- Все records в batch имеют одинаковый timestamp → `per_record_ts=0`, timestamp один раз в BatchHeader
- Хотя бы один record имеет отличный timestamp → `per_record_ts=1`, каждый record содержит свой

Когда пользователь не указывает `WithTimestamp()`, WAL auto-fills `time.Now().UnixNano()`.
Если все Append в batch произошли в один вызов — timestamps одинаковые → uniform mode → **8B overhead per record**.

## 3.3 Batch trailer

```
CRC32C  4B   covers bytes [0 .. BatchSize-5]
```

## 3.4 Полная раскладка frame

```
┌────────────────────────────────────────────────┐
│ BatchHeader                            28B     │
├────────────────────────────────────────────────┤
│ Records region (possibly compressed):          │
│   Record 0: [Timestamp?] KeyLen MetaLen        │
│             PayloadLen Key Meta Payload         │
│   Record 1: ...                                │
├────────────────────────────────────────────────┤
│ CRC32C                                  4B     │
└────────────────────────────────────────────────┘
```

## 3.5 Footprint summary

| Компонент | Размер |
|---|---|
| Batch header | **28B** |
| CRC | **4B** |
| **Total batch overhead** | **32B** |
| Per-record (uniform ts) | **8B** + variable |
| Per-record (individual ts) | **16B** + variable |

### Сравнение

| | old (v2) | new (v1) uniform ts | new (v1) per-record ts |
|---|---|---|---|
| Batch overhead | 28B | 32B (+4B) | 32B (+4B) |
| Per-record overhead | 6B | 8B (+2B for KeyLen) | 16B (+10B) |
| Timestamps | нет | batch-level | per-record |
| Key support | нет | да | да |

Common case (auto-timestamp, no explicit WithTimestamp):
**+2B per record** vs old format. Это цена за Key support.

---

# 4. Public API

## 4.1 Types

```go
type LSN = uint64
```

Глобальный монотонный идентификатор. Starts at 1. Zero = not assigned.

```go
type Event struct {
    LSN       LSN
    Timestamp int64   // UnixNano
    Key       []byte
    Meta      []byte
    Payload   []byte
}
```

При записи: LSN назначается WAL, Timestamp auto-fill если не задан.
При replay: все поля заполнены, Key/Meta/Payload — zero-copy mmap slices.

## 4.2 RecordOption

Shared между WAL.Append и Batch.Append / Batch.AppendUnsafe.

```go
type RecordOption func(*recordOptions)

func WithKey(key []byte) RecordOption
func WithMeta(meta []byte) RecordOption
func WithTimestamp(ts int64) RecordOption
func WithNoCompress() RecordOption
```

`WithNoCompress` помечает record. Если хотя бы один record в batch помечен —
весь batch пишется без сжатия (Flags bit 0 = 0). Это позволяет:
- пропустить сжатие для уже сжатых данных (JPEG, protobuf, zstd-encoded)
- явно управлять CPU vs size trade-off per event

## 4.3 Batch

```go
type Batch struct { /* unexported fields */ }

func NewBatch(n int) *Batch
func (b *Batch) Len() int
func (b *Batch) Reset()

// Append копирует payload, key, meta.
// Caller может переиспользовать буферы после вызова.
func (b *Batch) Append(payload []byte, opts ...RecordOption)

// AppendUnsafe берёт ownership. Zero-copy.
// Caller НЕ ДОЛЖЕН модифицировать slices после вызова.
func (b *Batch) AppendUnsafe(payload []byte, opts ...RecordOption)
```

## 4.4 WAL

### Lifecycle

```go
func Open(dir string, opts ...Option) (*WAL, error)
func (w *WAL) Shutdown(ctx context.Context) error  // graceful: drain, flush, fsync, close
func (w *WAL) Close() error                         // immediate: discard pending, close
```

`Open` принимает **директорию**. Создаётся при необходимости.

### Write

```go
// Single event. Всегда копирует payload.
// Timestamp = time.Now().UnixNano() если WithTimestamp не задан.
func (w *WAL) Append(payload []byte, opts ...RecordOption) (LSN, error)

// Batch. Один batch frame, одна CRC, атомарно.
func (w *WAL) AppendBatch(batch *Batch) (LSN, error)
```

### Read

```go
func (w *WAL) Replay(from LSN, fn func(Event) error) error
func (w *WAL) ReplayRange(from, to LSN, fn func(Event) error) error
func (w *WAL) ReplayBatches(from LSN, fn func(batch []Event) error) error
func (w *WAL) Iterator(from LSN) (*Iterator, error)
func (w *WAL) Follow(from LSN) (*Iterator, error)

type Iterator struct { /* unexported */ }
func (it *Iterator) Next() bool
func (it *Iterator) Event() Event
func (it *Iterator) Err() error
func (it *Iterator) Close() error
```

`ReplayBatches` — batch-level callback. Каждый вызов `fn` получает все events
одного batch frame. Для import/export, replication receiver, batch-aware consumers.

`Follow` — tail-follow iterator. Блокирует `Next()` когда дочитал до конца,
ждёт новых данных (fsnotify + polling fallback). Не завершается пока caller
не вызовет `Close()`. Позволяет строить real-time consumers, CDC pipelines,
replication followers.

### Flush & Sync

```go
func (w *WAL) Flush() error  // drain write queue → OS page cache
func (w *WAL) Sync() error   // fsync → disk
```

### Durability

```go
func (w *WAL) WaitDurable(lsn LSN) error
```

Блокирует до момента когда `lsn` будет fsync'd на диск. Для database commit
semantics: `INSERT → Append → WaitDurable → ack client`.

Внутри: coalesced fsync. Несколько горутин вызывают `WaitDurable` —
все ждут один общий fsync, не каждый свой. Это даёт throughput benefit
group commit для durability.

Работает с любым SyncMode:
- `SyncBatch` — WaitDurable возвращается сразу после того как batch fsync'd
- `SyncInterval` — может ждать до следующего sync tick
- `SyncNever` — WaitDurable триггерит one-shot fsync

### Segments

```go
func (w *WAL) Rotate() error
func (w *WAL) Segments() []SegmentInfo
func (w *WAL) DeleteBefore(lsn LSN) error

type SegmentInfo struct {
    Path           string
    FirstLSN       LSN
    LastLSN        LSN
    FirstTimestamp int64
    LastTimestamp   int64
    Size           int64
    CreatedAt      int64  // UnixNano
    Sealed         bool
}
```

### Snapshot

```go
func (w *WAL) Snapshot(fn func(ctrl *SnapshotController) error) error

type SnapshotController struct { /* unexported */ }
func (c *SnapshotController) Iterator() (*Iterator, error)
func (c *SnapshotController) IteratorFrom(lsn LSN) (*Iterator, error)
func (c *SnapshotController) Segments() []SegmentInfo
func (c *SnapshotController) Checkpoint(lsn LSN)
func (c *SnapshotController) Compact() error
```

### Replication

```go
// Открыть sealed segment для чтения (segment shipping).
// Возвращает io.ReadCloser — файл целиком, raw bytes.
func (w *WAL) OpenSegment(firstLSN LSN) (io.ReadCloser, SegmentInfo, error)

// Импортировать raw batch frame (от primary).
// Validates Magic + CRC. Assigns LSNs из frame (не генерирует новые).
func (w *WAL) ImportBatch(frame []byte) error

// Импортировать sealed segment файл целиком.
// Validates internal batch CRCs. Registers в manifest.
func (w *WAL) ImportSegment(path string) error
```

`OpenSegment` — для segment shipping replication (Kafka ISR style).
Primary вызывает `OpenSegment` → передаёт bytes по сети → replica вызывает `ImportSegment`.

`ImportBatch` — для batch-level replication (WAL tailing).
Primary пишет batch → replica получает raw frame → `ImportBatch`.
Тут replica **не перегенерирует LSN** — использует LSN из batch header.

`ImportSegment` — для segment-level replication.
Принимает path к файлу (уже скачанному), валидирует, регистрирует.

Оба `Import*` метода обходят append pipeline (direct write / file move).
Используют `WithStartLSN` для правильной LSN continuity на replica.

### Stats

```go
func (w *WAL) FirstLSN() LSN
func (w *WAL) LastLSN() LSN
func (w *WAL) Stats() Stats

type Stats struct {
    EventsWritten     uint64
    BatchesWritten    uint64
    BytesWritten      uint64
    BytesSynced       uint64
    SyncCount         uint64
    Drops             uint64
    CompressedBytes   uint64
    Corruptions       uint64
    QueueSize         int
    TotalSize         int64  // сумма всех сегментов
    ActiveSegmentSize int64  // размер текущего активного сегмента
    SegmentCount      int
    FirstLSN          LSN
    LastLSN           LSN
    State             State
}
```

---

# 5. Configuration

```go
w, err := uewal.Open(dir,
    // Segment rotation
    uewal.WithMaxSegmentSize(256 << 20),         // default 256 MB
    uewal.WithMaxSegmentAge(time.Hour),           // 0 = disabled (default)

    // Retention (все 0 = disabled = хранить всё)
    uewal.WithMaxSegments(100),
    uewal.WithRetentionSize(10 << 30),            // 10 GB
    uewal.WithRetentionAge(24 * time.Hour),

    // Durability
    uewal.WithSyncMode(uewal.SyncBatch),          // default SyncNever
    uewal.WithSyncInterval(50 * time.Millisecond), // for SyncInterval mode

    // Backpressure
    uewal.WithBackpressure(uewal.BlockMode),       // default
    uewal.WithQueueSize(4096),                     // default
    uewal.WithBufferSize(64 << 10),                // default 64 KB
    uewal.WithMaxBatchSize(4 << 20),               // default 4 MB, OOM protection

    // IO optimization
    uewal.WithPreallocate(true),                   // fallocate on segment creation

    // Replication
    uewal.WithStartLSN(100001),                    // replica: continue from primary's LSN

    // Extensions
    uewal.WithCompressor(comp),
    uewal.WithIndex(idx),
    uewal.WithHooks(hooks),
)
```

### Defaults

| Option | Default | Rationale |
|---|---|---|
| MaxSegmentSize | 256 MB | balance between rotation frequency and file size |
| MaxSegmentAge | 0 (disabled) | size-based rotation is sufficient for most use cases |
| MaxSegments | 0 (unlimited) | no data loss by default |
| RetentionAge | 0 (unlimited) | no data loss by default |
| RetentionSize | 0 (unlimited) | no data loss by default |
| SyncMode | SyncNever | maximum throughput; user opts-in to durability |
| SyncInterval | 100 ms | |
| Backpressure | BlockMode | predictable behavior |
| QueueSize | 4096 | |
| BufferSize | 64 KB | |
| MaxBatchSize | 4 MB | OOM protection; AppendBatch rejects batches exceeding this |
| Preallocate | false | true = fallocate segment to MaxSegmentSize at creation |
| StartLSN | 0 (auto) | 0 = start from 1 or continue from manifest; non-zero = set initial LSN counter (for replicas) |

---

# 6. Segments

## 6.1 Naming

Файл = первый LSN, 20 цифр zero-padded:

```
00000000000000000001.wal
00000000000000100001.wal
```

Natural lexicographic ordering. O(1) segment lookup by name.

## 6.2 Directory layout

```
wal-dir/
├── LOCK                             ← flock, prevents concurrent access
├── manifest.bin                     ← segment metadata + CRC
├── 00000000000000000001.wal         ← sealed segment
├── 00000000000000000001.idx         ← sparse index for sealed segment
├── 00000000000000050001.wal         ← sealed segment
├── 00000000000000050001.idx
├── 00000000000000100001.wal         ← active segment (writes go here)
└── 00000000000000100001.idx         ← sparse index (in-memory, persisted at seal)
```

Flat structure. Один LOCK файл на всю директорию.

## 6.3 Rotation

Triggers (first wins):

1. Segment size > `MaxSegmentSize`
2. Segment age > `MaxSegmentAge` (если > 0)
3. `w.Rotate()` (manual)

При ротации:

1. Seal active segment (read-only)
2. Persist sparse index → `.idx`
3. Create new segment (FirstLSN = prev.LastLSN + 1)
4. Update manifest (atomic write)
5. Run retention check

Rotation выполняется в writer goroutine (single-threaded, no races).

### Segment preallocation

Когда `WithPreallocate(true)`:

1. При создании сегмента: `fallocate(fd, 0, 0, MaxSegmentSize)` (Linux) или `SetEndOfFile` (Windows)
2. При seal: `ftruncate` до фактического размера

Это даёт:
- Contiguous allocation на диске → меньше fragmentation
- Меньше filesystem metadata writes при каждом append
- Быстрее sequential IO (prefetch friendly)

Используют: Kafka, Scylla, PostgreSQL WAL.

## 6.4 Retention

Выполняется после каждой ротации. Policies (все применяются):

| Policy | Condition | Action |
|---|---|---|
| By count | len(segments) > MaxSegments | delete oldest |
| By age | segment.CreatedAt + RetentionAge < now | delete |
| By size | totalSize > RetentionSize | delete oldest until fits |

Retention **пропускает** сегменты с refcount > 0 (active iterators).

## 6.5 Segment lifecycle

```
CREATE → ACTIVE → SEALED → DELETED
```

- ACTIVE: один, принимает writes
- SEALED: read-only, участвует в replay
- DELETED: файл + .idx удалены, metadata removed from manifest

---

# 7. Sparse Index

Per-segment. Одна запись на batch. Хранится в `.idx` файле.

## 7.1 Entry format

```
SparseEntry (24 bytes):
  FirstLSN   8B    ← batch FirstLSN        (8B aligned)
  Offset     8B    ← byte offset in .wal   (8B aligned)
  Timestamp  8B    ← batch timestamp (ns)  (8B aligned)
```

Массив SparseEntry, может быть mmap'd напрямую.

## 7.2 Операции

- **Find by LSN**: binary search → O(log B), B = batch count
- **Find by timestamp**: binary search → O(log B)
- **Build**: append entry after each batch write (in-memory for active segment)
- **Persist**: write to `.idx` at segment seal
- **Rebuild**: scan batch headers if `.idx` missing or corrupted

## 7.3 Sizing

| Batches | Index size |
|---|---|
| 1,000 | 24 KB |
| 10,000 | 240 KB |
| 100,000 | 2.4 MB |
| 1,000,000 | 24 MB |

Fits in L2/L3 cache for typical segment sizes.

---

# 8. Manifest

Binary format с CRC. Atomic update через write-to-temp + rename.

### Содержит

- Список сегментов (FirstLSN, LastLSN, Size, CreatedAt, Sealed, FirstTimestamp, LastTimestamp)
- Global LastLSN
- CRC-32C для integrity check

### Recovery with manifest

```
1. flock LOCK file
2. read manifest.bin, verify CRC
3. verify segment files exist on disk
4. load .idx files for sealed segments
5. validate active (last) segment: scan batch CRCs, truncate partial
6. rebuild in-memory sparse index for active segment
7. ready
```

### Recovery without manifest (cold start / manifest corrupt)

```
1. flock LOCK file
2. scan directory for *.wal, sort by name
3. for each segment: scan batch headers → build metadata
4. rebuild sparse indexes
5. validate last segment CRC, truncate partial
6. write manifest.bin
7. ready
```

---

# 9. Crash Recovery

### Active segment

1. Scan batch frames sequentially
2. Validate CRC per batch
3. Corruption detected → truncate to last valid batch boundary
4. Update manifest

### Partial write detection

Batch frame invalid if:
- Magic mismatch
- BatchSize extends past EOF
- CRC mismatch
- Truncated record data (records region shorter than declared)

All cases → truncate. Partial batch = discarded entirely (batch atomicity).

### Sealed segments

Не проверяются при каждом Open (CRC validated at seal time).
Manifest содержит checksums.

---

# 10. Snapshot

### Гарантии

- Callback выполняется с read access (writes продолжаются)
- Iterator видит consistent view sealed segments + active snapshot
- Compact удаляет только sealed segments полностью до checkpoint LSN
- Активный сегмент никогда не удаляется через Compact

### Пример: Database WAL

```go
err := w.Snapshot(func(ctrl *uewal.SnapshotController) error {
    iter, err := ctrl.IteratorFrom(lastCheckpointLSN)
    if err != nil {
        return err
    }
    defer iter.Close()

    var lastLSN uewal.LSN
    for iter.Next() {
        if err := applyToState(iter.Event()); err != nil {
            return err
        }
        lastLSN = iter.Event().LSN
    }
    if err := iter.Err(); err != nil {
        return err
    }

    ctrl.Checkpoint(lastLSN)
    return ctrl.Compact()
})
```

---

# 11. Append Pipeline

```
w.Append(payload, opts...)
  │
  ├── apply RecordOption → build record (copy payload)
  ├── lsnCounter.Add(1)  ← atomic, lock-free
  ├── writeQueue.enqueue(writeBatch)  ← backpressure
  │
  ▼
writer goroutine (single, sequential):
  ├── dequeueAll (group commit: drain all available batches)
  ├── for each batch: encode batch frame v1
  │     ├── decide per_record_ts flag (uniform or individual timestamps)
  │     ├── check WithNoCompress flag
  │     └── if Compressor && !noCompress: try compress → auto-bypass if no gain
  ├── write ALL encoded frames in single write() syscall
  ├── update in-memory sparse index (one entry per batch)
  ├── maybeSync (SyncBatch / SyncInterval / SyncNever)
  ├── wakeWaiters (WaitDurable: notify goroutines waiting for fsync'd LSN)
  ├── notifyIndexer (per-event callback)
  ├── check rotation condition → rotate if needed
  └── update stats
```

### Group commit = batch coalescing

Writer drains **все** pending batches из queue за один цикл, кодирует каждый
в отдельный batch frame (с отдельной CRC = batch atomicity), и пишет все frames
**одним write() syscall**. Это даёт throughput benefit coalescing без потери
per-batch CRC guarantees.

Отдельный merge frames в один frame НЕ делается: это сломало бы
атомарность каждого AppendBatch вызова (одна CRC = один AppendBatch = atomic unit).

---

# 12. Interfaces

### Compressor

```go
type Compressor interface {
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
}
```

Сжимает только records region. Header остаётся plaintext.
CRC covers compressed bytes. Implementations manage own buffers.

#### Auto-bypass

Encoder автоматически проверяет: если `len(compressed) >= len(original)` —
пишет original (Flags bit 0 = 0). Это гарантирует:
- Уже сжатые данные (JPEG, protobuf) не раздуваются
- Нет CPU waste на бесполезную компрессию
- Нулевой overhead для несжимаемых данных

Приоритет:
1. `WithNoCompress()` в batch → skip compress entirely (не вызывается Compress)
2. Compressor configured → try compress → auto-bypass if no gain
3. No Compressor → always uncompressed

### Indexer

```go
type IndexInfo struct {
    LSN       LSN      // event LSN
    Timestamp int64    // event timestamp (UnixNano)
    Key       []byte   // event key (nil if not set)
    Meta      []byte   // event metadata (nil if not set)
    Offset    int64    // batch frame byte offset within segment file
    Segment   LSN      // segment identifier (= segment FirstLSN)
}

type Indexer interface {
    OnAppend(info IndexInfo)
}
```

- Вызывается из writer goroutine после persist, per-event
- Один `defer recover()` на весь batch (не per-event closure)
- Key/Meta — borrowed slices, valid только во время вызова
- Payload не передаётся: для контента использовать Iterator

Позволяет строить:
- key → LSN lookup (broker consumer routing)
- timestamp → LSN index (time-range queries)
- aggregate index (event sourcing: meta=aggregate_type, key=aggregate_id)
- consumer offset tracking (broker: key=consumer_group)
- segment-aware routing (Segment field для cross-segment indexes)

### Storage

```go
type Storage interface {
    Write(p []byte) (n int, err error)
    Sync() error
    Close() error
    Size() (int64, error)
    Truncate(size int64) error
    ReadAt(p []byte, off int64) (n int, err error)
}
```

Default: FileStorage с flock. Используется per-segment.

### Hooks

```go
type Hooks struct {
    // Lifecycle — called in caller / main goroutine
    OnStart         func()
    OnShutdownStart func()
    OnShutdownDone  func(elapsed time.Duration)

    // Write pipeline — called in writer goroutine, must not block
    AfterAppend     func(firstLSN, lastLSN LSN, count int)
    BeforeWrite     func(bytes int)
    AfterWrite      func(bytes int, elapsed time.Duration)
    BeforeSync      func()
    AfterSync       func(bytes int, elapsed time.Duration)

    // Errors
    OnCorruption    func(segmentPath string, offset int64)
    OnDrop          func(count int)

    // Segment lifecycle — called in writer goroutine
    OnRotation      func(sealed SegmentInfo)
    OnDelete        func(deleted SegmentInfo)
}
```

12 hooks. Все optional. Все panic-safe (recover внутри hooksRunner).

Hooks вызываются синхронно — **не должны блокировать writer goroutine**.
Для тяжёлой работы: channel + отдельная горутина.

#### Параметры hooks — обоснование

| Hook | Параметры | Зачем |
|---|---|---|
| `AfterAppend` | firstLSN, lastLSN, count | LSN range для метрик/логов ("batch 100-109, 10 events") |
| `BeforeWrite` | bytes | размер буфера, для monitoring write batch size |
| `AfterWrite` | bytes, elapsed | throughput metrics (MB/s = bytes/elapsed) |
| `BeforeSync` | — | сигнал начала fsync (для latency spans) |
| `AfterSync` | bytes, elapsed | fsync latency metrics, bytes synced for durability tracking |
| `OnCorruption` | segmentPath, offset | какой сегмент, где corruption (для alerting) |
| `OnDrop` | count | сколько событий потеряно (для backpressure monitoring) |
| `OnRotation` | sealed SegmentInfo | полная инфо о sealed segment (для replication triggers) |
| `OnDelete` | deleted SegmentInfo | полная инфо об удалённом (для cleanup tracking) |

---

# 13. Safety

### Directory lock

Один `LOCK` файл в WAL директории (flock / LockFileEx).
Предотвращает concurrent access от другого процесса.
Отдельные сегменты НЕ лочатся.

### mmap refcounting

Каждый segment handle имеет atomic refcount:
- `Iterator.open(segment)` → refcount++
- `Iterator.Close()` / segment transition → refcount--
- Retention пропускает segments с refcount > 0

### Atomic manifest

Write-to-temp + rename. На POSIX: atomic. На Windows: best-effort (write + close + rename).

---

# 14. Errors

```go
var (
    ErrClosed             // operation on closed WAL
    ErrDraining           // Append during graceful shutdown
    ErrNotRunning         // operation requires StateRunning
    ErrCorrupted          // CRC mismatch during iteration
    ErrQueueFull          // Append in ErrorMode when queue full
    ErrDirectoryLocked    // WAL directory locked by another process
    ErrInvalidLSN         // invalid LSN argument
    ErrShortWrite         // storage Write returned 0 without error
    ErrInvalidRecord      // truncated or unsupported batch frame
    ErrCRCMismatch        // CRC-32C validation failure
    ErrInvalidState       // illegal lifecycle transition
    ErrEmptyBatch         // Append/AppendBatch with zero events
    ErrCompressorRequired // compressed frame without Compressor configured
    ErrLSNOutOfRange      // ReplayRange with to <= from
    ErrBatchTooLarge      // encoded batch exceeds MaxBatchSize
    ErrSegmentNotFound    // OpenSegment with unknown firstLSN
    ErrImportInvalid      // ImportBatch/ImportSegment: magic/CRC mismatch
)
```

17 sentinel errors. Все comparable с `==` и `errors.Is`.
Нет internal errors в public API.

---

# 15. File Structure

```
uewal/
├── wal.go              WAL struct, Open, Shutdown, Close, Rotate
├── append.go           Append, AppendBatch, lsnCounter, eventSlicePool
├── event.go            LSN, Event, Batch, RecordOption, NewBatch
├── encoding.go         batch frame v1 encode/decode/scan, encoder
├── replay.go           Iterator, Replay, ReplayRange, cross-segment iteration
├── segment.go          segmentWriter: per-file lifecycle, seal, delete, prealloc
├── manager.go          segmentManager: rotation, retention, lookup
├── manifest.go         manifest read/write, atomic update
├── sparse.go           sparseIndex, sparseEntry, binary search
├── snapshot.go         SnapshotController, Snapshot method
├── follow.go           Follow iterator (tail-follow with blocking)
├── replication.go      OpenSegment, ImportBatch, ImportSegment
├── durable.go          WaitDurable, coalesced fsync
├── options.go          config, Option, SyncMode, BackpressureMode, defaults
├── storage.go          Storage interface, FileStorage
├── state.go            stateMachine (INIT → RUNNING → DRAINING → CLOSED)
├── stats.go            Stats, statsCollector (atomic counters)
├── hooks.go            Hooks, hooksRunner (panic-safe)
├── errors.go           sentinel errors
├── queue.go            writeQueue (bounded, backpressure)
├── writer.go           single writer goroutine, group commit
├── mmap.go             mmapReader abstraction
├── mmap_unix.go        syscall.Mmap (Linux, macOS)
├── mmap_windows.go     CreateFileMapping / MapViewOfFile
├── mmap_fallback.go    ReadAt fallback for custom Storage
├── flock.go            fileLock type
├── flock_unix.go       flock(2)
├── flock_windows.go    LockFileEx / UnlockFileEx
├── doc.go              package documentation
├── internal/crc/       CRC-32C (Castagnoli, hardware accelerated)
│
├── encoding_test.go    frame v1 encode/decode/scan, flag combinations
├── append_test.go      Append, AppendBatch, RecordOption, backpressure
├── replay_test.go      Replay, ReplayRange, Iterator, cross-segment
├── segment_test.go     segment lifecycle, seal, delete
├── manager_test.go     rotation, retention, segment lookup
├── manifest_test.go    manifest persistence, corruption fallback
├── sparse_test.go      build, binary search, LSN/timestamp lookups
├── snapshot_test.go    Snapshot, Checkpoint, Compact
├── follow_test.go      Follow iterator, blocking, new events, Close
├── replication_test.go OpenSegment, ImportBatch, ImportSegment, CRC validation
├── durable_test.go     WaitDurable coalescing, SyncMode interaction
├── wal_test.go         lifecycle, Open/Shutdown/Close, directory lock
├── writer_test.go      group commit, sync modes, rotation trigger
├── storage_test.go     FileStorage
├── mmap_test.go        mmap/munmap
├── hooks_test.go       invocation, panic safety, new hooks
├── stats_test.go       atomic counters, new fields
├── state_test.go       state transitions
├── queue_test.go       writeQueue, backpressure modes
├── stress_test.go      concurrent append + replay + rotation + snapshot
├── fuzz_test.go        fuzz decode, fuzz recovery
├── bench_test.go       write, read, encoding, rotation, cross-segment
├── example_test.go     runnable examples for godoc
└── helpers_test.go     shared test utilities (memStorage, etc.)
```

---

# 16. Performance Targets

| Metric | Target |
|---|---|
| Append latency (async, 128B) | < 500 ns |
| Append throughput (128B) | > 3M ops/sec |
| Batch append (100 x 128B) | > 1 GB/s |
| Replay throughput (mmap) | > 4 GB/s |
| Encoding (batch, no compress) | > 5 GB/s |
| Recovery (1M events, header scan) | < 200 ms |
| Rotation latency | < 1 ms |
| Sparse index lookup | < 1 us |
| Allocs per Append | 0 |
| Allocs per encode | 0 |
| Allocs per decode (reused buf) | 0 |

---

# 17. Use Cases

| Use case | Key features used |
|---|---|
| Message broker | Key routing, retention, Follow, Indexer for consumer offsets |
| Event sourcing | Replay, Snapshot/Checkpoint, time-range queries, ReplayBatches |
| Database WAL | Commit log, WaitDurable, Snapshot + Compact for truncation |
| CDC pipeline | Follow, Key-based filtering via Indexer, replay from offset |
| Durable queue | Append + Follow, backpressure, retention, WaitDurable |
| Replication | OpenSegment + ImportSegment (segment shipping), ImportBatch (WAL tailing) |

---

# 18. Design Decisions

Conscious trade-offs, задокументированные для прозрачности.

### RecordCount uint16 (не uint32)

65535 records per batch. При 100B records = 6.5MB batch, при 10B = 655KB.
Достаточно для любого реального use case. Kafka default batch size = 1MB.
Экономия: 2B per batch header. При необходимости: split на несколько AppendBatch.

### Нет RecordSize prefix

Record size вычисляется из `KeyLen + MetaLen + PayloadLen + header overhead` за O(1).
Record-level skip не нужен: sparse index работает на уровне batch, не record.
Экономия: 4B per record. При 1M records = 4MB.

### Channel-based queue (не lock-free ring buffer)

Текущая реализация writeQueue достаточно быстра (channel + mutex).
Lock-free ring buffer (Disruptor-style) даёт ~10-20% latency improvement,
но значительно усложняет код. Можно добавить в v2 если профилирование покажет bottleneck.

### Нет low watermark API

`DeleteBefore(lsn)` уже позволяет callerу удалять старые данные.
Multi-consumer watermark tracking — ответственность caller, не WAL.
WAL не должен знать о consumer semantics. Для брокеров: Indexer + caller logic.

### Compression: только records region

```
Header (plaintext, всегда читаем)
Records (compressed, CRC covers compressed bytes)
CRC
```

Header остаётся plaintext для:
- recovery scan (batch headers без decompress)
- sparse index build (FirstLSN, Timestamp из header)
- frame boundary detection (Magic + BatchSize)

### Auto-compression bypass

Encoder вызывает `Compressor.Compress()`, затем сравнивает:
- `len(compressed) >= len(original)` → пишет original, Flags bit 0 = 0
- `len(compressed) < len(original)` → пишет compressed, Flags bit 0 = 1

Нет ручного переключения "compress on/off" в конфиге.
Есть `WithNoCompress()` per-record/batch — skip compress call entirely.
Auto-bypass — safety net: даже без `WithNoCompress()` WAL не раздует данные.

### WaitDurable: coalesced fsync

Вместо fsync per goroutine — один fsync для всех waiters:

```
goroutine A: WaitDurable(100) ──► wait ─┐
goroutine B: WaitDurable(105) ──► wait ─┤── one fsync() ──► all unblock
goroutine C: WaitDurable(108) ──► wait ─┘
```

Это стандартная техника (PostgreSQL commit delay, etcd batcher).
Без коалесцинга: N goroutines → N fsync → bottleneck.
С коалесцингом: N goroutines → 1 fsync → амортизация.

### Follow iterator: blocking semantics

`Follow` iterator при достижении конца лога:
1. Подписывается на уведомления от writer goroutine (channel)
2. Fallback: fsnotify watch на активный сегмент
3. Fallback: polling (configurable interval)

`Next()` блокирует, но уважает context cancellation через `Close()`.
При ротации: Follow автоматически открывает новый сегмент.

### ImportBatch: frame validation

`ImportBatch` не проходит через append pipeline:
- Прямой write в active segment
- Validates Magic, Version, CRC
- LSN берётся из batch header (не генерируется)
- Sparse index update, hooks fire, manifest update

Это осознанный trade-off: Import path не идёт через queue/batcher,
потому что replica получает уже сформированные frames от primary.

### Нет built-in WAL-level indexes

Sparse index (LSN/Timestamp → offset) — единственный встроенный индекс.
Все domain-specific indexes (key lookup, bloom filter, consumer offsets) —
ответственность `Indexer` callback. WAL предоставляет данные, не интерпретирует.

Обоснование:
- WAL не знает семантику key (это может быть UUID, aggregate_id, partition key)
- Bloom filter per-segment полезен, но увеличивает footprint и complexity
- Indexer API достаточно мощный для любого domain-specific index

---

# 19. v2+ Extensibility Points (задел в v1 коде)

v1 реализация ОБЯЗАНА содержать следующие точки расширения,
чтобы v2+ features добавлялись без breaking changes в API и wire format:

| v2+ Feature | Задел в v1 коде |
|---|---|
| Log compaction | 6 reserved flag bits. segmentManager.sealSegment() — hook point для merge |
| Tiered storage | segmentWriter принимает Storage через constructor, не создаёт сам |
| Lock-free queue | writeQueue за internal interface, реализация swappable |
| Double buffered writer | encoder.encodeBatch() возвращает []byte, writer.write() принимает []byte — фазы разделены |
| Segment handle pool | segmentManager.openSegment() / closeSegment() — clean lifecycle API |
| Bloom filter | naming: .wal + .idx + .bloom — convention в коде, manifest extensible |
| Encryption at rest | Flags bit reserved. Encryptor interface добавляется рядом с Compressor без breaking |
| Consensus helpers | manifest format version field + optional extra section для term/vote |

### API stability guarantees

- `WAL`, `Event`, `Batch`, `Iterator`, `LSN` — public types, stable
- `Option`, `RecordOption` — functional options, additive only
- `Hooks` struct — new fields = backward compatible (nil = noop)
- `Indexer`, `Compressor`, `Storage` — interface stability, methods only added via new interface
- Wire format: Version byte + Flags bits = new features without breaking readers
- Manifest: version field + forward-compatible parsing (skip unknown sections)

---

# 20. Future Extensions (v2+)

Архитектура v1 не должна блокировать:

| Feature | Механизм расширения |
|---|---|
| Log compaction | new flag bit + merge sealed segments |
| Tiered storage | custom Storage per sealed segment |
| Partitioning | multiple WAL instances, key routing in Indexer |
| Consumer groups | Indexer-based offset management |
| Low watermark | persistent min-consumer-offset for auto-retention |
| Lock-free queue | ring buffer (Disruptor-style) for lower latency |
| Double buffered writer | overlap encoding (buffer A) + I/O (buffer B) for 5-15% throughput gain |
| Segment handle pool | keep N recently accessed segments open, avoid open/close overhead |
| Bloom filter | per-segment bloom filter for key existence queries |
| Encryption at rest | flag bit + Encryptor interface (like Compressor) |
| Consensus helpers | Raft/Paxos integration (term, vote log) |

Moved to v1 (из ранних v2+ планов):
- Replication helpers: `OpenSegment`, `ImportBatch`, `ImportSegment`, `WithStartLSN`
- WAL streaming: `Follow` iterator
- Batch-level replay: `ReplayBatches`
- Durability: `WaitDurable`
- Compression control: `WithNoCompress`, auto-bypass

---

# 20. Implementation Phases

### Phase 1 — Core Refactoring

Изменения в существующем коде (breaking changes):

1. Event: добавить Key, Timestamp fields
2. RecordOption: WithKey, WithMeta, WithTimestamp, WithNoCompress
3. Batch: Append(payload, opts...) + AppendUnsafe(payload, opts...) вместо Add/AddWithMeta
4. WAL.Append: `(payload []byte, opts ...RecordOption)` вместо `(events ...Event)`
5. Encoding: batch frame v1 (28B header, per_record_ts flag, 8B/16B per record)
6. Compressor: auto-bypass (compressed >= original → store original)
7. Indexer: struct-based `OnAppend(IndexInfo)` вместо loose params
8. Hooks: убрать BeforeAppend, обогатить AfterAppend/AfterWrite/AfterSync/OnCorruption
9. Обновить все тесты под новый API

Результат: рабочий single-segment WAL с новым API и форматом.

### Phase 2 — Segmented WAL

Новые файлы:

1. segment.go — segmentWriter (per-file lifecycle)
2. manager.go — segmentManager (rotation, retention, lookup)
3. manifest.go — manifest persistence (binary + CRC, atomic write)
4. sparse.go — sparseIndex (in-memory + .idx persistence)
5. WAL.Open: принимает директорию, LOCK file, manifest, segments
6. Writer: rotation trigger, sparse index update
7. Replay: cross-segment iteration with refcounting
8. Новые тесты

Результат: полнофункциональный сегментированный WAL.

### Phase 3 — Advanced Features

1. snapshot.go — SnapshotController
2. durable.go — WaitDurable, coalesced fsync
3. follow.go — Follow iterator (tail-follow with blocking)
4. replication.go — OpenSegment, ImportBatch, ImportSegment
5. ReplayBatches — batch-level callback
6. ReplayRange с sparse index acceleration
7. WithStartLSN — LSN seeding for replicas
8. Crash recovery для segmented mode (manifest + fallback scan)

Результат: полнофункциональный commit log engine с replication и durability.

### Phase 4 — Hardening

1. Stress tests (concurrent append + rotation + replay + snapshot + follow)
2. Fuzz tests (decode, recovery, import validation)
3. Benchmarks (rotation, cross-segment replay, sparse lookup, WaitDurable)
4. Edge cases: Follow across rotation, Import during retention, WaitDurable timeout
5. Documentation, examples, README update

Результат: production-ready commit log engine.
