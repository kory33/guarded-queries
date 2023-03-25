package io.github.kory33.guardedqueries.core.utils.extensions;

import java.util.stream.Stream;

public class StreamExtensions {
    private StreamExtensions() {
    }

    /**
     * Concat multiple streams into a single stream.
     */
    @SafeVarargs
    public static <T> Stream<T> concatAll(Stream<? extends T>... streams) {
        return Stream.of(streams).flatMap(s -> s);
    }
}
