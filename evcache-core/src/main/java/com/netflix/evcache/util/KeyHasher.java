package com.netflix.evcache.util;

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

    private static Logger log = LoggerFactory.getLogger(KeyHasher.class);
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

    public static String getHashedKey(String key, String hashingAlgorithm) {
        final long start = System.nanoTime();
        HashFunction hf = null; 
        switch(hashingAlgorithm.toLowerCase()) {
            case "murmur3" :
                hf = Hashing.murmur3_128();
                break;
    
            case "adler32" :
                hf = Hashing.adler32();
                break;
    
            case "crc32" :
                hf = Hashing.crc32();
                break;
    
            case "sha1" :
                hf = Hashing.sha1();
                break;
    
            case "sha256" :
                hf = Hashing.sha256();
                break;
    
            case "siphash24" :
                hf = Hashing.sipHash24();
                break;

            case "goodfasthash" :
                hf = Hashing.goodFastHash(128);
                break;
    
            case "md5" :
            default :
                hf = Hashing.md5();
                break;
        }

        final HashCode hc = hf.newHasher().putString(key, Charsets.UTF_8).hash();
        final byte[] digest = hc.asBytes();
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; digest length : " + digest.length + "; byte Array contents : " + Arrays.toString(digest) );
        final String hKey = encoder.encodeToString(digest);
        if(log.isDebugEnabled()) log.debug("Key : " + key +"; Hashed & encoded key : " + hKey + "; Took " + (System.nanoTime() - start) + " nanos");
        return hKey;
    }
}
