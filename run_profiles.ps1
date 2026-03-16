$ErrorActionPreference = "Continue"
$profileDir = ".\profiles"

$benchmarks = @(
    # --- Write path: payload sizes ---
    "BenchmarkAppend_PayloadOnly_64B",
    "BenchmarkAppend_PayloadOnly_128B",
    "BenchmarkAppend_PayloadOnly_1KB",
    "BenchmarkAppend_PayloadOnly_4KB",
    "BenchmarkAppend_PayloadOnly_64KB",
    # --- Write path: record options ---
    "BenchmarkAppend_NoOptions",
    "BenchmarkAppend_OnlyKey",
    "BenchmarkAppend_OnlyMeta",
    "BenchmarkAppend_OnlyTimestamp",
    "BenchmarkAppend_OnlyNoCompress",
    "BenchmarkAppend_AllRecordOptions",
    "BenchmarkAppend_KeyMeta_NoTimestamp",
    "BenchmarkAppend_KeyTimestamp_NoMeta",
    "BenchmarkAppend_EmptyPayload",
    # --- Write path: batch sizes ---
    "BenchmarkBatchAppend_Size1",
    "BenchmarkBatchAppend_Size10",
    "BenchmarkBatchAppend_Size100",
    "BenchmarkBatchAppend_Size1000",
    # --- Batch semantics: Copy vs Unsafe ---
    "BenchmarkBatchAppend_CopySemantics_100",
    "BenchmarkBatchAppend_UnsafeSemantics_100",
    "BenchmarkBatch_CopySemanticsViaWAL_10",
    "BenchmarkBatch_UnsafeSemanticsViaWAL_10",
    "BenchmarkBatch_CopySemanticsViaWAL_1000",
    "BenchmarkBatch_UnsafeSemanticsViaWAL_1000",
    # --- Batch variants ---
    "BenchmarkBatchAppend_WithKeyMeta_100",
    "BenchmarkBatchAppend_Reuse_100",
    "BenchmarkBatchAppend_LargePayload4KB_100",
    "BenchmarkBatchAppend_MixedTimestamps_100",
    # --- Sync modes ---
    "BenchmarkAppend_SyncNever",
    "BenchmarkAppend_SyncBatch",
    "BenchmarkAppend_SyncInterval10ms",
    "BenchmarkAppend_SyncInterval100ms",
    "BenchmarkFlush_SingleEvent",
    "BenchmarkFlushThenSync",
    "BenchmarkWaitDurable_SyncNever",
    "BenchmarkWaitDurable_SyncBatch",
    # --- Compression ---
    "BenchmarkAppend_NoCompression",
    "BenchmarkAppend_NopCompressor",
    "BenchmarkAppend_ShrinkCompressor50pct",
    "BenchmarkAppend_WithNoCompressFlag",
    "BenchmarkBatchAppend_Compression_100",
    # --- Buffer & Queue sizing ---
    "BenchmarkAppend_BufferSize4KB",
    "BenchmarkAppend_BufferSize64KB",
    "BenchmarkAppend_BufferSize1MB",
    "BenchmarkAppend_QueueSize64",
    "BenchmarkAppend_QueueSize4096",
    "BenchmarkAppend_QueueSize16384",
    # --- Backpressure ---
    "BenchmarkAppend_BackpressureBlock",
    "BenchmarkAppend_BackpressureDrop",
    "BenchmarkAppend_BackpressureError",
    # --- Parallel writers ---
    "BenchmarkAppendParallel_1Writer",
    "BenchmarkAppendParallel_4Writers",
    "BenchmarkAppendParallel_16Writers",
    "BenchmarkAppendParallel_SyncBatch",
    "BenchmarkBatchAppendParallel_100",
    "BenchmarkAppendParallel_WithKeyMeta",
    # --- Encoding ---
    "BenchmarkEncode_10Records_128B",
    "BenchmarkEncode_100Records_128B",
    "BenchmarkEncode_1000Records_128B",
    "BenchmarkEncode_100Records_4KB",
    "BenchmarkEncode_UniformTimestamp",
    "BenchmarkEncode_PerRecordTimestamp",
    "BenchmarkEncode_WithKeyMeta",
    "BenchmarkEncode_WithCompressor",
    "BenchmarkEncodeRecordsRegion_100x128B",
    "BenchmarkEncodeRecordsRegion_PerRecTS",
    "BenchmarkEncodeBatchFrame_Direct_100x128B",
    # --- Decoding ---
    "BenchmarkDecode_10Records_128B",
    "BenchmarkDecode_100Records_128B",
    "BenchmarkDecode_1000Records_128B",
    "BenchmarkDecode_100Records_4KB",
    "BenchmarkDecode_PerRecordTimestamp",
    "BenchmarkDecodeInto_BufferReuse_100",
    "BenchmarkDecodeInto_NoBufferReuse_100",
    "BenchmarkDecodeInto_WithDecompressor",
    "BenchmarkScanBatchFrame_HeaderOnly",
    "BenchmarkScanBatchFrame_LargeFrame",
    # --- Replay ---
    "BenchmarkReplay_10KEvents_256B",
    "BenchmarkReplay_100KEvents_256B",
    "BenchmarkReplay_100KEvents_128B",
    "BenchmarkReplay_FromMiddle_100KEvents",
    "BenchmarkReplayRange_Last1K_Of100K",
    "BenchmarkReplayBatches_100KEvents",
    "BenchmarkReplay_Batched_1KBatches_100",
    # --- Iterator ---
    "BenchmarkIterator_10KEvents",
    "BenchmarkIterator_100KEvents",
    "BenchmarkIterator_FromMiddle_100K",
    "BenchmarkIterator_ReadPayload_100K",
    # --- Recovery ---
    "BenchmarkRecovery_1KEvents",
    "BenchmarkRecovery_100KEvents",
    "BenchmarkRecovery_LargePayload4KB_10K",
    # --- Rotation ---
    "BenchmarkRotation_Manual",
    "BenchmarkRotation_AutoBySize_1MB",
    "BenchmarkAppend_SmallSegment256KB",
    "BenchmarkAppend_LargeSegment256MB",
    # --- Preallocate ---
    "BenchmarkAppend_Preallocate_On",
    "BenchmarkAppend_Preallocate_Off",
    # --- WAL Options: MaxBatchSize, SegmentAge, MaxSegments, Retention, StartLSN ---
    "BenchmarkAppend_MaxBatchSize1MB",
    "BenchmarkAppend_MaxBatchSize16KB",
    "BenchmarkAppend_MaxSegmentAge1s",
    "BenchmarkAppend_MaxSegments5_WithRotation",
    "BenchmarkAppend_RetentionSize512KB",
    "BenchmarkAppend_RetentionAge5s",
    "BenchmarkAppend_StartLSN1000",
    "BenchmarkAppend_StartLSN1M",
    # --- Sparse index ---
    "BenchmarkSparseIndex_FindByLSN_100Entries",
    "BenchmarkSparseIndex_FindByLSN_10KEntries",
    "BenchmarkSparseIndex_FindByLSN_100KEntries",
    "BenchmarkSparseIndex_FindByTimestamp_100Entries",
    "BenchmarkSparseIndex_FindByTimestamp_10KEntries",
    "BenchmarkSparseIndex_Append",
    "BenchmarkSparseIndex_Marshal_1KEntries",
    "BenchmarkSparseIndex_Unmarshal_1KEntries",
    "BenchmarkSparseIndex_MarshalUnmarshal_10KEntries",
    # --- Queue ---
    "BenchmarkQueue_EnqueueDequeue_Paired",
    "BenchmarkQueue_TryEnqueue_Fast",
    "BenchmarkQueue_Contended_8Producers",
    "BenchmarkQueue_SmallCapacity_64",
    "BenchmarkQueue_LargeCapacity_16384",
    # --- Durable notifier ---
    "BenchmarkDurableNotifier_AdvanceNoWaiters",
    "BenchmarkDurableNotifier_WaitAlreadySynced",
    "BenchmarkDurableNotifier_AdvanceWithWaiters",
    "BenchmarkDurableNotifier_ConcurrentWaiters_10",
    # --- Manifest ---
    "BenchmarkManifest_Marshal_10Segments",
    "BenchmarkManifest_Marshal_100Segments",
    "BenchmarkManifest_Marshal_1000Segments",
    "BenchmarkManifest_Unmarshal_10Segments",
    "BenchmarkManifest_Unmarshal_100Segments",
    "BenchmarkManifest_Unmarshal_1000Segments",
    # --- CRC ---
    "BenchmarkCRC32C_64B",
    "BenchmarkCRC32C_1KB",
    "BenchmarkCRC32C_64KB",
    "BenchmarkCRC32C_1MB",
    # --- Memory / Pool ---
    "BenchmarkRecordSlicePool_GetPut",
    "BenchmarkRecordSlicePool_GetPut_LargeSlice",
    "BenchmarkBatch_NewAndFill_100",
    "BenchmarkBatch_ResetAndFill_100",
    "BenchmarkCopyBytes_128B",
    "BenchmarkCopyBytes_4KB",
    "BenchmarkApplyOptions_NoOpts",
    "BenchmarkApplyOptions_WithKeyMeta",
    # --- Encoder ---
    "BenchmarkEncoder_Reset",
    "BenchmarkEncoder_MultipleBatches_Accumulation",
    "BenchmarkEncoder_GrowFromSmall",
    "BenchmarkEncoder_PreSized",
    # --- Hooks / Indexer ---
    "BenchmarkAppend_NoHooks",
    "BenchmarkAppend_AllHooks",
    "BenchmarkAppend_NoIndexer",
    "BenchmarkAppend_WithIndexer",
    "BenchmarkBatchAppend_WithIndexer_100",
    # --- Lifecycle ---
    "BenchmarkOpen_EmptyDir",
    "BenchmarkOpen_WithManifest_100Segments",
    "BenchmarkShutdown_WithPendingEvents",
    # --- E2E / Throughput ---
    "BenchmarkE2E_AppendThenReplay_1KEvents",
    "BenchmarkE2E_BatchAppendThenIterator_10KEvents",
    "BenchmarkThroughput_Burst_10K_128B",
    "BenchmarkThroughput_Burst_10K_1KB",
    "BenchmarkThroughput_BatchBurst_100Batches_100Events",
    "BenchmarkThroughput_ParallelBurst"
)

$total = $benchmarks.Count
$i = 0

foreach ($bench in $benchmarks) {
    $i++
    Write-Host "[$i/$total] $bench" -ForegroundColor Cyan
    $cpuFile = "$profileDir\cpu_$bench.prof"
    $memFile = "$profileDir\mem_$bench.prof"
    go test -run='^$' -bench="^${bench}$" -benchtime=3s -count=1 `
        -cpuprofile="$cpuFile" `
        -memprofile="$memFile" `
        -timeout=120s 2>&1 | Out-Host
}

Write-Host "`nDone! $total profiles written to $profileDir" -ForegroundColor Green
