package io.github.kory33.guardedqueries.core.utils.extensions;

import org.apache.commons.lang3.tuple.Pair;

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

    /**
     * Zip each element of a stream with its index (in the order traversed by the iterator) starting from 0.
     */
    public static <T> Stream<Pair<T, Long>> zipWithIndex(final Stream<T> stream) {
        return IteratorExtensions.stream(IteratorExtensions.zipWithIndex(stream.iterator()));
    }
}
