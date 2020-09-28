package com.netflix.evcache.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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
        goodfasthash,
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

//    public static String getHashedKey1(String key, String hashingAlgorithm) {
//        try {
//            final MessageDigest messageDigest = MessageDigest.getInstance(hashingAlgorithm);
//            messageDigest.update(key.getBytes(), 0, key.length());
//            final byte[] digest = messageDigest.digest();
//            if(log.isDebugEnabled()) log.debug("Key : " + key +"; digest length : " + digest.length + "; byte Array contents : " + Arrays.toString(digest));
//            final String hKey = encoder.encodeToString(digest);
//            if(log.isDebugEnabled()) log.debug("Key : " + key +"; Hashed & encoded key : " + hKey);
//            return hKey;
//        } catch(NoSuchAlgorithmException ex){
//            log.error("Exception while trying to conver key to its hash", ex);
//            return key;
//        }
//    }

    public static String getHashedKeyEncoded(String key, HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes, Integer maxHashLength) {
        final long start = System.nanoTime();
        byte[] digest = getHashedKey(key, hashingAlgorithm, maxDigestBytes);
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; digest length : " + digest.length + "; byte Array contents : " + Arrays.toString(digest) );
        String hKey = encoder.encodeToString(digest);
        if (null != hKey && maxHashLength != null && maxHashLength > 0 && maxHashLength < hKey.length()) {
            hKey = hKey.substring(0, maxHashLength);
        }
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; Hashed & encoded key : " + hKey + "; Took " + (System.nanoTime() - start) + " nanos");
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

            case goodfasthash:
                hf = Hashing.goodFastHash(128);
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
}
