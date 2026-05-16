# Compression Algorithm Enum — Design Spec

**Date:** 2026-05-16
**Scope:** `evcache-core` — `EVCacheSerializingTranscoder`, `EVCacheTranscoder`, `build.gradle`

---

## Goal

Add first-class support for pluggable compression algorithms (gzip, zstd) in EVCache, while preserving full backward compatibility with existing gzip-compressed data already in cache.

---

## Changes

### 1. `EVCacheSerializingTranscoder` — enum, constant, field, constructors, compress/decompress

**Nested enum:**
```java
public enum CompressionAlgorithm { GZIP, ZSTD }
```

**Constant:**
```java
public static final int DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;
```
Level 3 is zstd's own default — good speed/ratio balance for a cache write path. Level 9 (VHS's choice) is 10x slower to compress for only ~10% better ratio, appropriate for at-rest storage but not a latency-sensitive cache.

**New field:**
```java
private final CompressionAlgorithm compressionAlgorithm;
```
Defaults to `GZIP` in existing constructors — no behavioral change for current callers.

**New constructors:**
```java
// existing — unchanged, default to GZIP
public EVCacheSerializingTranscoder()                          // → this(MAX_SIZE)
public EVCacheSerializingTranscoder(int max)                   // → this(max, GZIP)

// new
public EVCacheSerializingTranscoder(int max, CompressionAlgorithm algo)
public EVCacheSerializingTranscoder(int max, CompressionAlgorithm algo, int zstdLevel)
```

**`compress()` override — dispatches on `compressionAlgorithm`:**
- `GZIP`: `GZIPOutputStream` at Java default level (6) — identical to current `BaseSerializingTranscoder` behavior
- `ZSTD`: `Zstd.compress(data, zstdLevel)`

**`decompress()` override — magic-byte auto-detection (modeled on VHS `CompressionUtils`):**

| Magic bytes | Algorithm |
|---|---|
| `0x1F 0x8B` | gzip |
| `0x28 0xB5 0x2F 0xFD` (little-endian int) | zstd |
| neither | return data as-is (backward compat) |

Zstd decompression uses a fast path when the frame carries a content-size header (`Zstd.decompressedSize() > 0`), falling back to `ZstdInputStream` stream-decode otherwise.

The `compressionAlgorithm` field is **not consulted during decode** — detection is always by magic bytes. This ensures any transcoder instance can decode data written by any other instance regardless of its configured algorithm.

**Metrics:** `updateTimerWithCompressionRatio` replaces hardcoded `"gzip"` tag with `compressionAlgorithm.name().toLowerCase()`.

---

### 2. `EVCacheTranscoder` — two new FPs, new constructors

**New FP — algorithm:**
```
Property: default.evcache.compression.algorithm
Type:     String
Default:  "GZIP"
```
Cast to enum via `CompressionAlgorithm.valueOf(value.toUpperCase())`. Throws `IllegalArgumentException` for unsupported values — fast-fail at startup rather than silent misconfiguration.

**New FP — zstd level:**
```
Property: default.evcache.compression.zstd.level
Type:     Integer
Default:  EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL  (3)
```
Only used when algorithm is `ZSTD`. Gzip level is not exposed — Java default (6) is the established behavior in this repo.

**New constructors threading both values through to super:**
```java
public EVCacheTranscoder(int max)
public EVCacheTranscoder(int max, int compressionThreshold)
public EVCacheTranscoder(int max, int compressionThreshold, CompressionAlgorithm algo, int zstdLevel)
```

The no-arg and `(int max)` constructors read both FPs and delegate to the full constructor.

---

### 3. `evcache-core/build.gradle` — new dependency

```groovy
api group: 'com.github.luben', name: 'zstd-jni', version: 'latest.release'
```

Same library used by `viewing_history_service/CompressionUtils.java`.

---

## Backward Compatibility

- All existing constructors default to `GZIP` — no behavior change for current callers.
- `decompress()` auto-detects by magic bytes, so existing gzip-compressed cache entries decode correctly even if the transcoder is reconfigured to write zstd.
- The `COMPRESSED` flag bit is unchanged — no wire format changes.

---

## Out of Scope

- Gzip level FP (Java default level 6 is the established behavior; easy to add later)
- Updating `ChunkTranscoder` (separate concern)

