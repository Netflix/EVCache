package com.netflix.evcache.test.transcoder;

import java.util.Objects;

public class Movie {
    long id;
    String name;

    public Movie() { // required for decode
    }

    public Movie(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Movie)) return false;
        Movie movie = (Movie) o;
        return id == movie.id && Objects.equals(name, movie.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
