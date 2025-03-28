package com.netflix.evcache.pool;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.HashAlgorithm;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Description of a hash ring algorithm. A hash ring algorithm has a hash
 * function, and a way to split the hash into parts in the case of a
 * ketama-like algorithm.
 */
public interface HashRingAlgorithm {

    long hash(CharSequence key);

    int getCountHashParts();

    void getHashPartsInto(CharSequence key, long[] parts);

    static class SimpleHashRingAlgorithm implements HashRingAlgorithm {
        HashAlgorithm hashAlgorithm;

        public SimpleHashRingAlgorithm(HashAlgorithm hashAlgorithm) {
            this.hashAlgorithm = hashAlgorithm;
        }

        @Override
        public long hash(CharSequence key) {
            return hashAlgorithm.hash(key.toString());
        }

        @Override
        public int getCountHashParts() {
            return 1;
        }

        @Override
        public void getHashPartsInto(CharSequence key, long[] parts) {
            parts[0] = hash(key);
        }
    }

    static class KetamaMd5HashRingAlgorithm implements HashRingAlgorithm {
        private static MessageDigest md5Digest = null;

        static {
            try {
                md5Digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("MD5 not supported", e);
            }
        }

        @Override
        public long hash(CharSequence key) {
            // manually inlined from DefaultHashAlgorithm, avoids the Charset lookup
            long rv = 0;
            MessageDigest md5;
            try {
              md5 = (MessageDigest) md5Digest.clone();
            } catch (CloneNotSupportedException e) {
              throw new RuntimeException("clone of MD5 not supported", e);
            }
            md5.update(key.toString().getBytes(StandardCharsets.UTF_8));
            byte[] bKey = md5.digest();
            rv = ((long) (bKey[3] & 0xFF) << 24)
                    | ((long) (bKey[2] & 0xFF) << 16)
                    | ((long) (bKey[1] & 0xFF) << 8)
                    | (bKey[0] & 0xFF);
            return rv & 0xffffffffL; /* Truncate to 32-bits */
        }

        @Override
        public int getCountHashParts() {
            return 4;
        }

        @Override
        public void getHashPartsInto(CharSequence key, long[] parts) {
            byte[] digest = DefaultHashAlgorithm.computeMd5(key.toString());
            for (int h = 0; h < 4; h++) {
                parts[h] = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                        | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                        | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                        | (digest[h * 4] & 0xFF);
            }
        }
    }
}
