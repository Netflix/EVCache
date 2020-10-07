package com.netflix.evcache.util;

import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fzakaria.ascii85.Ascii85;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.netflix.archaius.api.Property;

public class KeyHasher {
    /**
     * meta data size 
     * 40 + key + 'item_hdr' size
i.e.
40 + keysize + 12
And if client flags are present:
40 + keysize + 4 bytes(for flags) + 12
And if CAS and client flags are present:
40 + keysize + 4 bytes(for flags) + 8(for CAS) + 12
     */

    public enum HashingAlgorithm {
        murmur3,
        adler32,
        crc32,
        sha1,
        sha256,
        siphash24,
        md5,
        NO_HASHING // useful for disabling hashing at client level, while Hashing is enabled at App level
    }

    public static HashingAlgorithm getHashingAlgorithmFromString(String algorithmStr) {
        try {
            if (null == algorithmStr || algorithmStr.isEmpty()) {
                return null;
            }
            return HashingAlgorithm.valueOf(algorithmStr.toLowerCase());
        } catch (IllegalArgumentException ex) {
            // default to md5 incase of unsupported algorithm
            return HashingAlgorithm.md5;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(KeyHasher.class);
    private static final Encoder encoder= Base64.getEncoder().withoutPadding();

    public static String getHashedKeyEncoded(String key, HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes, Integer maxHashLength) {
        return getHashedKeyEncoded(key, hashingAlgorithm, maxDigestBytes, maxHashLength, null);
    }
    public static String getHashedKeyEncoded(String key, HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes, Integer maxHashLength, String baseEncoder) {
        final long start = System.nanoTime();
        byte[] digest = getHashedKey(key, hashingAlgorithm, maxDigestBytes);
        if(log.isDebugEnabled()) {
            final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
            char[] hexChars = new char[digest.length * 2];
            for (int j = 0; j < digest.length; j++) {
                int v = digest[j] & 0xFF;
                hexChars[j * 2] = HEX_ARRAY[v >>> 4];
                hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
            }
            log.debug("Key : " + key +"; hex : " + new String(hexChars));
        }
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; digest length : " + digest.length + "; byte Array contents : " + Arrays.toString(digest) );
        String hKey = null;
        if(baseEncoder != null && baseEncoder.equals("ascii85")) {
            hKey = Ascii85.encode(digest);
            if(log.isDebugEnabled()) log.debug("Key : " + key +"; Hashed & Ascii85 encoded key : " + hKey + "; Took " + (System.nanoTime() - start) + " nanos");
        } else {
            hKey = encoder.encodeToString(digest);
            if (null != hKey && maxHashLength != null && maxHashLength > 0 && maxHashLength < hKey.length()) {
                hKey = hKey.substring(0, maxHashLength);
            }
            if(log.isDebugEnabled()) log.debug("Key : " + key +"; Hashed & encoded key : " + hKey + "; Took " + (System.nanoTime() - start) + " nanos");
        } 

        return hKey;
    }

    public static byte[] getHashedKeyInBytes(String key, HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes) {
        final long start = System.nanoTime();
        byte[] digest = getHashedKey(key, hashingAlgorithm, maxDigestBytes);
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; digest length : " + digest.length + "; byte Array contents : " + Arrays.toString(digest) + "; Took " + (System.nanoTime() - start) + " nanos");
        return digest;
    }

    private static byte[] getHashedKey(String key, HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes) {
        HashFunction hf = null;
        switch (hashingAlgorithm) {
            case murmur3:
                hf = Hashing.murmur3_128();
                break;

            case adler32:
                hf = Hashing.adler32();
                break;

            case crc32:
                hf = Hashing.crc32();
                break;

            case sha1:
                hf = Hashing.sha1();
                break;

            case sha256:
                hf = Hashing.sha256();
                break;

            case siphash24:
                hf = Hashing.sipHash24();
                break;

            case md5:
            default:
                hf = Hashing.md5();
                break;
        }

        final HashCode hc = hf.newHasher().putString(key, Charsets.UTF_8).hash();
        final byte[] digest = hc.asBytes();

        if (maxDigestBytes != null && maxDigestBytes > 0 && maxDigestBytes < digest.length) {
            return Arrays.copyOfRange(digest, 0, maxDigestBytes);
        }

        return digest;
    }

    
    public static void main(String args[]) {
        
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%d{HH:mm:ss,SSS} [%t] %p %c %x - %m%n")));
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);

        String key = "MAP_LT:721af5a5-3452-4b62-86fb-5f31ccde8d99_187978153X28X2787347X1601330156682";
        System.out.println(getHashedKeyEncoded(key, HashingAlgorithm.murmur3, null, null));
    }

}
