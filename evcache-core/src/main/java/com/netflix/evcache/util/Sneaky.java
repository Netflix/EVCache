package com.netflix.evcache.util;

/**
 * Sneaky can be used to sneakily throw checked exceptions without actually declaring this in your method's throws clause.
 * This somewhat contentious ability should be used carefully, of course.
 */
public class Sneaky {
    public static RuntimeException sneakyThrow(Throwable t) {
        if ( t == null ) throw new NullPointerException("t");
        Sneaky.<RuntimeException>sneakyThrow0(t);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow0(Throwable t) throws T {
        throw (T)t;
    }
}
