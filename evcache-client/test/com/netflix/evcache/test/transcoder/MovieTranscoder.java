package com.netflix.evcache.test.transcoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovieTranscoder implements Transcoder<Movie> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MovieTranscoder.class);
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean asyncDecode(CachedData d) {
        return false;
    }

    @Override
    public CachedData encode(Movie movie) {
        byte[] bytes;
        try {
            bytes = mapper.writeValueAsBytes(movie);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new CachedData(0, bytes, bytes.length);
    }

    @Override
    public Movie decode(CachedData d) {
        try {
            return mapper.readValue(d.getData(), Movie.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }
}
