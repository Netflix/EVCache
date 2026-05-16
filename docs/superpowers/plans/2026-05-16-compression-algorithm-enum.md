# Compression Algorithm Enum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `CompressionAlgorithm` enum (GZIP, ZSTD) to `EVCacheSerializingTranscoder` with magic-byte auto-detection on decode and FP-driven algorithm/level selection in `EVCacheTranscoder`.

**Architecture:** The enum and all compress/decompress logic live in `EVCacheSerializingTranscoder`. Existing constructors default to GZIP — no behavioral change for current callers. Decode always auto-detects by magic bytes regardless of the configured algorithm, ensuring backward compatibility. `EVCacheTranscoder` reads two new FPs (`default.evcache.compression.algorithm`, `default.evcache.compression.zstd.level`) and threads them into the parent constructor.

**Tech Stack:** Java, TestNG, `com.github.luben:zstd-jni`, `java.util.zip.GZIPOutputStream/GZIPInputStream`, `java.nio.ByteBuffer`

---

## Files

| Action | Path |
|---|---|
| Modify | `evcache-core/build.gradle` |
| Modify | `evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java` |
| Modify | `evcache-core/src/main/java/com/netflix/evcache/EVCacheTranscoder.java` |
| Create | `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java` |
| Modify | `evcache-core/src/test/java/test-suite.xml` |

---

## Task 1: Add zstd-jni dependency

**Files:**
- Modify: `evcache-core/build.gradle`

- [ ] **Step 1: Add dependency**

In `evcache-core/build.gradle`, add after the last `api` line in the `dependencies` block:

```groovy
api group: 'com.github.luben', name: 'zstd-jni', version: 'latest.release'
```

- [ ] **Step 2: Verify it resolves**

```bash
./gradlew :evcache-core:dependencies --configuration compileClasspath | grep zstd
```

Expected output includes a line like:
```
\--- com.github.luben:zstd-jni:...
```

- [ ] **Step 3: Commit**

```bash
git add evcache-core/build.gradle
git commit -m "build: add zstd-jni dependency to evcache-core"
```

---

## Task 2: Add enum, constants, fields, and constructors

**Files:**
- Modify: `evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java`
- Create: `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`
- Modify: `evcache-core/src/test/java/test-suite.xml`

- [ ] **Step 1: Create the test file with failing tests**

Create `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`:

```java
package com.netflix.evcache;

import net.spy.memcached.CachedData;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EVCacheSerializingTranscoderTest {

    @Test
    public void testEnumValues() {
        assertEquals(EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("GZIP"),
                EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        assertEquals(EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("ZSTD"),
                EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
    }

    @Test
    public void testDefaultZstdLevelConstant() {
        assertEquals(EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL, 3);
    }

    @Test
    public void testDefaultConstructorUsesGzip() {
        // Default constructor must not throw; algorithm defaults to GZIP
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder();
        assertNotNull(t);
    }

    @Test
    public void testConstructorWithAlgorithm() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE,
                EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        assertNotNull(t);
    }

    @Test
    public void testConstructorWithAlgorithmAndLevel() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE,
                EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD,
                5);
        assertNotNull(t);
    }
}
```

- [ ] **Step 2: Register the test class in test-suite.xml**

In `evcache-core/src/test/java/test-suite.xml`, add inside `<suite>`:

```xml
<test name="TranscoderTests">
  <classes>
    <class name="com.netflix.evcache.EVCacheSerializingTranscoderTest" />
  </classes>
</test>
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: compilation errors — `CompressionAlgorithm` and `DEFAULT_ZSTD_COMPRESSION_LEVEL` don't exist yet.

- [ ] **Step 4: Add enum, constant, fields, and constructors to `EVCacheSerializingTranscoder`**

Replace the existing two constructors and add new fields. The complete updated top section of the class (after the existing `static final` flag constants and before `asyncDecode`) should look like this:

```java
    public enum CompressionAlgorithm { GZIP, ZSTD }

    public static final int DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;

    private static final int ZSTD_MAGIC = 0xFD2FB528;
    private static final byte GZIP_MAGIC_0 = (byte) 0x1f;
    private static final byte GZIP_MAGIC_1 = (byte) 0x8b;

    private final TranscoderUtils tu = new TranscoderUtils(true);
    private Timer timer;
    private final CompressionAlgorithm compressionAlgorithm;
    private final int zstdLevel;

    public EVCacheSerializingTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    public EVCacheSerializingTranscoder(int max) {
        this(max, CompressionAlgorithm.GZIP);
    }

    public EVCacheSerializingTranscoder(int max, CompressionAlgorithm algo) {
        this(max, algo, DEFAULT_ZSTD_COMPRESSION_LEVEL);
    }

    public EVCacheSerializingTranscoder(int max, CompressionAlgorithm algo, int zstdLevel) {
        super(max);
        this.compressionAlgorithm = algo;
        this.zstdLevel = zstdLevel;
    }
```

Also add these imports at the top of the file:

```java
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
```

Remove the old two constructors:
```java
// DELETE these:
public EVCacheSerializingTranscoder() {
    this(CachedData.MAX_SIZE);
}
public EVCacheSerializingTranscoder(int max) {
    super(max);
}
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL` and `TranscoderTests` pass.

- [ ] **Step 6: Commit**

```bash
git add evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java \
        evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java \
        evcache-core/src/test/java/test-suite.xml
git commit -m "feat: add CompressionAlgorithm enum, constants, fields, and constructors to EVCacheSerializingTranscoder"
```

---

## Task 3: Override compress()

**Files:**
- Modify: `evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java`
- Modify: `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`

- [ ] **Step 1: Add failing tests**

Add these test methods to `EVCacheSerializingTranscoderTest`:

```java
    @Test
    public void testGzipEncodeSetsGzipMagicBytes() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        t.setCompressionThreshold(0); // compress everything
        // Use a String — encode() compresses when above threshold
        CachedData encoded = t.encode("hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "Expected gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "Expected gzip magic byte 1");
    }

    @Test
    public void testZstdEncodeSetsZstdMagicBytes() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        // Zstd magic is 0xFD2FB528 in little-endian: bytes 0x28 0xB5 0x2F 0xFD
        assertEquals(data[0], (byte) 0x28, "Expected zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "Expected zstd magic byte 1");
        assertEquals(data[2], (byte) 0x2F, "Expected zstd magic byte 2");
        assertEquals(data[3], (byte) 0xFD, "Expected zstd magic byte 3");
    }
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: both new tests fail — `EVCacheSerializingTranscoder` still delegates to `BaseSerializingTranscoder.compress()` which always writes gzip, so the zstd test fails.

- [ ] **Step 3: Add compress() override and helpers**

Add these methods to `EVCacheSerializingTranscoder`, before `updateTimerWithCompressionRatio`:

```java
    @Override
    protected byte[] compress(byte[] in) {
        if (in == null) throw new NullPointerException("Can't compress null");
        switch (compressionAlgorithm) {
            case ZSTD:
                return Zstd.compress(in, zstdLevel);
            case GZIP:
            default:
                return compressGzip(in);
        }
    }

    private byte[] compressGzip(byte[] in) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(in.length / 10);
        GZIPOutputStream gz = null;
        try {
            gz = new GZIPOutputStream(bos);
            gz.write(in);
        } catch (IOException e) {
            throw new RuntimeException("IO exception compressing data", e);
        } finally {
            closeQuietly(gz);
            closeQuietly(bos);
        }
        return bos.toByteArray();
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) try { c.close(); } catch (IOException ignored) {}
    }
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java \
        evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java
git commit -m "feat: override compress() in EVCacheSerializingTranscoder to dispatch on algorithm"
```

---

## Task 4: Override decompress() with magic-byte auto-detection

**Files:**
- Modify: `evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java`
- Modify: `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`

- [ ] **Step 1: Add failing tests**

Add these test methods to `EVCacheSerializingTranscoderTest`:

```java
    @Test
    public void testGzipRoundTrip() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        t.setCompressionThreshold(0);
        String original = "round trip gzip round trip gzip round trip gzip";
        CachedData encoded = t.encode(original);
        assertEquals(t.decode(encoded), original);
    }

    @Test
    public void testZstdRoundTrip() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        t.setCompressionThreshold(0);
        String original = "round trip zstd round trip zstd round trip zstd";
        CachedData encoded = t.encode(original);
        assertEquals(t.decode(encoded), original);
    }

    @Test
    public void testGzipTranscoderDecodesZstdData() {
        // Encode with ZSTD
        EVCacheSerializingTranscoder writer = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        writer.setCompressionThreshold(0);
        CachedData encoded = writer.encode("cross decode test cross decode test cross decode test");

        // Decode with a GZIP-configured transcoder — must auto-detect zstd
        EVCacheSerializingTranscoder reader = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        assertEquals(reader.decode(encoded), "cross decode test cross decode test cross decode test");
    }

    @Test
    public void testZstdTranscoderDecodesGzipData() {
        // Encode with GZIP (legacy data already in cache)
        EVCacheSerializingTranscoder writer = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        writer.setCompressionThreshold(0);
        CachedData encoded = writer.encode("legacy gzip data legacy gzip data legacy gzip data");

        // Decode with a ZSTD-configured transcoder — must auto-detect gzip
        EVCacheSerializingTranscoder reader = new EVCacheSerializingTranscoder(
                CachedData.MAX_SIZE, EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        assertEquals(reader.decode(encoded), "legacy gzip data legacy gzip data legacy gzip data");
    }
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `testZstdRoundTrip` and `testGzipTranscoderDecodesZstdData` fail — zstd-compressed bytes are fed to the gzip decompressor in the parent class, which throws.

- [ ] **Step 3: Add decompress() override and helpers**

Add these methods to `EVCacheSerializingTranscoder`, after `compressGzip` and before `updateTimerWithCompressionRatio`:

```java
    @Override
    protected byte[] decompress(byte[] in) {
        if (in == null) return null;
        if (isGzipCompressed(in)) return decompressGzip(in);
        if (isZstdCompressed(in)) return decompressZstd(in);
        return in;
    }

    private boolean isGzipCompressed(byte[] data) {
        return data.length >= 2 && data[0] == GZIP_MAGIC_0 && data[1] == GZIP_MAGIC_1;
    }

    private boolean isZstdCompressed(byte[] data) {
        if (data.length < 4) return false;
        int magic = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return magic == ZSTD_MAGIC;
    }

    private byte[] decompressGzip(byte[] in) {
        ByteArrayInputStream bis = new ByteArrayInputStream(in);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(in.length * 2);
        GZIPInputStream gis = null;
        try {
            gis = new GZIPInputStream(bis);
            byte[] buf = new byte[8192];
            int r;
            while ((r = gis.read(buf)) > 0) {
                bos.write(buf, 0, r);
            }
        } catch (IOException e) {
            getLogger().warn("Failed to decompress gzip data", e);
            return null;
        } finally {
            closeQuietly(gis);
            closeQuietly(bis);
            closeQuietly(bos);
        }
        return bos.toByteArray();
    }

    private byte[] decompressZstd(byte[] in) {
        try {
            long originalSize = Zstd.decompressedSize(in);
            if (originalSize > Integer.MAX_VALUE) {
                getLogger().warn("Zstd frame declares size > Integer.MAX_VALUE: {}", originalSize);
                return null;
            }
            if (originalSize > 0) {
                return Zstd.decompress(in, (int) originalSize);
            }
            // Slow path: frame has no content-size header — stream decode
            ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(in));
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                byte[] buf = new byte[8192];
                int n;
                while ((n = zis.read(buf)) != -1) {
                    bos.write(buf, 0, n);
                }
                return bos.toByteArray();
            } finally {
                zis.close();
            }
        } catch (IOException e) {
            getLogger().warn("Failed to decompress zstd data", e);
            return null;
        }
    }
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java \
        evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java
git commit -m "feat: override decompress() with magic-byte auto-detection for gzip and zstd"
```

---

## Task 5: Update compression metrics tag

**Files:**
- Modify: `evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java`
- Modify: `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`

Note: `EVCacheMetricsFactory` requires a running registry so we verify this change by inspection only — no unit test for the metric tag itself.

- [ ] **Step 1: Update the hardcoded `"gzip"` tag**

In `updateTimerWithCompressionRatio`, replace:

```java
tagList.add(new BasicTag(EVCacheMetricsFactory.COMPRESSION_TYPE, "gzip"));
```

with:

```java
tagList.add(new BasicTag(EVCacheMetricsFactory.COMPRESSION_TYPE, compressionAlgorithm.name().toLowerCase()));
```

- [ ] **Step 2: Run all tests to confirm nothing broke**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add evcache-core/src/main/java/com/netflix/evcache/EVCacheSerializingTranscoder.java
git commit -m "feat: use algorithm name in compression metrics tag instead of hardcoded gzip"
```

---

## Task 6: Update EVCacheTranscoder with new FPs and constructors

**Files:**
- Modify: `evcache-core/src/main/java/com/netflix/evcache/EVCacheTranscoder.java`
- Modify: `evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java`

- [ ] **Step 1: Add failing tests**

Add these test methods to `EVCacheSerializingTranscoderTest`:

```java
    @Test
    public void testEVCacheTranscoderExplicitZstd() {
        EVCacheTranscoder t = new EVCacheTranscoder(
                CachedData.MAX_SIZE, 0,
                EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD,
                EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL);
        String original = "evcachetranscoder zstd test evcachetranscoder zstd test";
        CachedData encoded = t.encode(original);
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0);
        // Verify zstd magic bytes
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28);
        assertEquals(data[1], (byte) 0xB5);
        assertEquals(data[2], (byte) 0x2F);
        assertEquals(data[3], (byte) 0xFD);
        assertEquals(t.decode(encoded), original);
    }

    @Test
    public void testEVCacheTranscoderExplicitGzip() {
        EVCacheTranscoder t = new EVCacheTranscoder(
                CachedData.MAX_SIZE, 0,
                EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP,
                EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL);
        String original = "evcachetranscoder gzip test evcachetranscoder gzip test";
        CachedData encoded = t.encode(original);
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0);
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f);
        assertEquals(data[1], (byte) 0x8b);
        assertEquals(t.decode(encoded), original);
    }
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: compilation errors — the four-arg constructor on `EVCacheTranscoder` doesn't exist yet.

- [ ] **Step 3: Rewrite EVCacheTranscoder**

Replace the full contents of `EVCacheTranscoder.java` with:

```java
package com.netflix.evcache;

import com.netflix.evcache.util.EVCacheConfig;
import net.spy.memcached.CachedData;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    public EVCacheTranscoder() {
        this(EVCacheConfig.getInstance().getPropertyRepository()
                .get("default.evcache.max.data.size", Integer.class)
                .orElse(20 * 1024 * 1024).get());
    }

    public EVCacheTranscoder(int max) {
        this(max, EVCacheConfig.getInstance().getPropertyRepository()
                .get("default.evcache.compression.threshold", Integer.class)
                .orElse(120).get());
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        this(max, compressionThreshold,
                CompressionAlgorithm.valueOf(
                        EVCacheConfig.getInstance().getPropertyRepository()
                                .get("default.evcache.compression.algorithm", String.class)
                                .orElse("GZIP").get().toUpperCase()),
                EVCacheConfig.getInstance().getPropertyRepository()
                        .get("default.evcache.compression.zstd.level", Integer.class)
                        .orElse(DEFAULT_ZSTD_COMPRESSION_LEVEL).get());
    }

    public EVCacheTranscoder(int max, int compressionThreshold, CompressionAlgorithm algo, int zstdLevel) {
        super(max, algo, zstdLevel);
        setCompressionThreshold(compressionThreshold);
    }

    @Override
    public boolean asyncDecode(CachedData d) {
        return super.asyncDecode(d);
    }

    @Override
    public Object decode(CachedData d) {
        return super.decode(d);
    }

    @Override
    public CachedData encode(Object o) {
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
./gradlew :evcache-core:test 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add evcache-core/src/main/java/com/netflix/evcache/EVCacheTranscoder.java \
        evcache-core/src/test/java/com/netflix/evcache/EVCacheSerializingTranscoderTest.java
git commit -m "feat: add compression algorithm and zstd level FPs to EVCacheTranscoder"
```
