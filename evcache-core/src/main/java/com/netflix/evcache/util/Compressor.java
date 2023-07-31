package com.netflix.evcache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.DistributionSummary;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;

public class Compressor {
    private static final Logger log = LoggerFactory.getLogger(Compressor.class);
    public static final String CLIENT_ALGO = "client.algo";
    public static final String CLIENT_COMP_RATIO = "Client_CompressionRatio";
    public static final String ORIG_SIZE = "Client_UncompressedSize";
    public static final String COMP_SIZE = "Client_CompressedSize";
    public static final String APP_NAME = "Client.appName";
    public Map<String, DistributionSummary> distributionSummaryCompRatioMap = new ConcurrentHashMap<>();
    public Map<String, DistributionSummary> distributionSummaryOrigSizeMap = new ConcurrentHashMap<>();
    public Map<String, DistributionSummary> distributionSummaryCompSizeMap = new ConcurrentHashMap<>();
    private static Timer timer;
    public byte[] compress(byte[] data, String algorithm, int level, String appName) throws IOException {
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
            case "gzip":
                ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(gzipBaos);
                gzipOut.write(data);
                gzipOut.close();
                compressedData = gzipBaos.toByteArray();
                break;
            default:
                throw new IllegalArgumentException("Invalid compression algorithm: " + algorithm);
        }

        // higher ratio means better compressed and vice-versa
        double ratio = (double) data.length / compressedData.length;
        log.debug("compression ratio: " + ratio + "for app: " + appName);
        final List<Tag> tagList = new ArrayList<Tag>(2);
        tagList.add(new BasicTag(CLIENT_ALGO, algorithm));
        tagList.add(new BasicTag(APP_NAME, appName));
        DistributionSummary distributionSummaryCompRatio = this.distributionSummaryCompRatioMap.get(appName);
        if (distributionSummaryCompRatio == null) {
            distributionSummaryCompRatio = EVCacheMetricsFactory.getInstance().getDistributionSummary(CLIENT_COMP_RATIO, tagList);
            this.distributionSummaryCompRatioMap.put(appName, distributionSummaryCompRatio);
        }
        distributionSummaryCompRatio.record((long) ratio);

        final List<Tag> tagList2 = new ArrayList<Tag>(1);
        tagList2.add(new BasicTag(APP_NAME, appName));
        DistributionSummary distributionSummaryOrigSize =
                this.distributionSummaryOrigSizeMap.get(appName);
        if (distributionSummaryOrigSize == null) {
            distributionSummaryOrigSize = EVCacheMetricsFactory.getInstance().getDistributionSummary(ORIG_SIZE, tagList2);
            this.distributionSummaryOrigSizeMap.put(appName, distributionSummaryOrigSize);
        }
        distributionSummaryOrigSize.record(data.length);

        DistributionSummary distributionSummaryCompSize = this.distributionSummaryCompSizeMap.get(appName);
        if (distributionSummaryCompSize == null) {
            distributionSummaryCompSize = EVCacheMetricsFactory.getInstance().getDistributionSummary(COMP_SIZE, tagList2);
            this.distributionSummaryCompSizeMap.put(appName, distributionSummaryCompSize);
        }
        distributionSummaryCompSize.record(compressedData.length);
        if (ratio > 1) {
            return compressedData;
        }
        return data;
    }


    public byte[] decompress(byte[] data, String algorithm) throws IOException {
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

            case "gzip":
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
            default:
                throw new IllegalArgumentException("Invalid compression algorithm: " + algorithm);
        }
        return decompressedData;
    }
}
