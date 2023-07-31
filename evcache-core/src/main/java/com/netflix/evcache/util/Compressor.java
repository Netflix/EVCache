package com.netflix.evcache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.netflix.spectator.api.BasicTag;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;

public class Compressor {
    private static final Logger log = LoggerFactory.getLogger(Compressor.class);
    private static Timer timer;
    public byte[] compress(byte[] data, String algorithm, int level) throws IOException {
        byte[] compressedData = null;
        switch (algorithm) {
            case "zstd":
                ByteArrayOutputStream zstdBaos = new ByteArrayOutputStream();
                if (level == 0) {
                    level = Zstd.defaultCompressionLevel();
                }
                ZstdOutputStream zstdOut = new ZstdOutputStream(zstdBaos, level);
                zstdOut.write(data);
                zstdOut.close();
                compressedData = zstdBaos.toByteArray();
                break;
            case "lz4":
                LZ4Factory factory = LZ4Factory.fastestInstance();
                LZ4Compressor compressor = factory.fastCompressor();
                compressedData = new byte[compressor.maxCompressedLength(data.length)];
                int compressedLength = compressor.compress(data, 0, data.length, compressedData, 0, compressedData.length);
                compressedData = java.util.Arrays.copyOfRange(compressedData, 0, compressedLength);
                break;
            case "snappy":
                ByteArrayOutputStream snappyBaos = new ByteArrayOutputStream();
                SnappyOutputStream snappyOut = new SnappyOutputStream(snappyBaos);
                snappyOut.write(data);
                snappyOut.close();
                compressedData = snappyBaos.toByteArray();
                break;
            default:
                ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(gzipBaos);
                gzipOut.write(data);
                gzipOut.close();
                compressedData = gzipBaos.toByteArray();
                break;
        }
        // higher ratio means better compressed and vice-versa
        double ratio = (double) data.length / compressedData.length;
        System.out.println("compression ratio: " + ratio);
        log.info("compression ratio: " + ratio);
        if (timer == null) {
            final List<Tag> tagList = new ArrayList<Tag>(1);
            tagList.add(new BasicTag("repl.algo", algorithm));
            timer = EVCacheMetricsFactory.getInstance().getPercentileTimer("repl.ratio", tagList, Duration.ofMillis(100));
        }

        timer.record((long) ratio, TimeUnit.MILLISECONDS);
        if (ratio > 1)
            return compressedData;
        return data;
    }


    public static byte[] decompress(byte[] data, String algorithm) throws IOException {
        byte[] decompressedData = null;
        int len;
        switch (algorithm) {
            case "zstd":
                ByteArrayInputStream zstdBais = new ByteArrayInputStream(data);
                ZstdInputStream zstdIn = new ZstdInputStream(zstdBais);
                byte[] buffer = new byte[4096];
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((len = zstdIn.read(buffer)) > 0) {
                    baos.write(buffer, 0, len);
                }
                decompressedData = baos.toByteArray();
                zstdIn.close();
                baos.close();
                break;
            case "lz4":
                LZ4Factory factory = LZ4Factory.fastestInstance();
                LZ4SafeDecompressor decompressor = factory.safeDecompressor();
                int decompressedLength = decompressor.decompress(data, 0, data.length, null, 0);
                decompressedData = new byte[decompressedLength];
                decompressor.decompress(data, 0, data.length, decompressedData, 0);
                break;
            case "snappy":
                decompressedData = Snappy.uncompress(data);
                break;

            default:
                ByteArrayInputStream gzipBais = new ByteArrayInputStream(data);
                GZIPInputStream gzipIn = new GZIPInputStream(gzipBais);
                ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream();
                byte[] buf = new byte[4096];
                while ((len = gzipIn.read(buf)) > 0) {
                    gzipBaos.write(buf, 0, len);
                }
                decompressedData = gzipBaos.toByteArray();
                gzipIn.close();
                gzipBaos.close();
                break;
        }
        return decompressedData;
    }
}
