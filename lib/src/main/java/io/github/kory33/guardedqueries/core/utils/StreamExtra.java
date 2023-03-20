package io.github.kory33.guardedqueries.core.utils;

import java.util.stream.Stream;

public class StreamExtra {
    private StreamExtra() {
    }

    /**
     * Concat multiple streams into a single stream.
     */
    @SafeVarargs
    public static <T> Stream<T> concatAll(Stream<? extends T>... streams) {
        return Stream.of(streams).flatMap(s -> s);
    }
}
