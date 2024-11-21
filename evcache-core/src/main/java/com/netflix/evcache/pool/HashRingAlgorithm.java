package com.netflix.evcache.pool;

import net.openhft.hashing.LongTupleHashFunction;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.HashAlgorithm;

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
        @Override
        public long hash(CharSequence key) {
            return DefaultHashAlgorithm.KETAMA_HASH.hash(key.toString());
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

    static class KetamaMurmur3HashRingAlgorithm implements HashRingAlgorithm {
        static final LongTupleHashFunction murmur3 = LongTupleHashFunction.murmur_3();

        @Override
        public long hash(CharSequence key) {
            long[] results = new long[2];
            murmur3.hashChars(key, results);
            return results[1] & 0xffffffffL; /* Truncate to 32-bits */
        }

        @Override
        public int getCountHashParts() {
            return 4;
        }

        @Override
        public void getHashPartsInto(CharSequence key, long[] parts) {
            long[] results = new long[2];
            murmur3.hashChars(key, results);

            // Split the two 64-bit values into four 32-bit chunks
            parts[0] = results[0] & 0xffffffffL; // Lower 32 bits of first long
            parts[1] = (results[0] >>> 32) & 0xffffffffL; // Upper 32 bits of first long
            parts[2] = results[1] & 0xffffffffL; // Lower 32 bits of second long
            parts[3] = (results[1] >>> 32) & 0xffffffffL; // Upper 32 bits of second long
        }
    }
}
