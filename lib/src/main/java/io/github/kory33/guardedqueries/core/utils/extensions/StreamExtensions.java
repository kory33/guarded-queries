package io.github.kory33.guardedqueries.core.utils.extensions;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class StreamExtensions {
    private StreamExtensions() {
    }
    
    /**
     * Zip each element of a stream with its index (in the order traversed by the iterator) starting from 0.
     */
    public static <T> Stream<Pair<T, Long>> zipWithIndex(final Stream<T> stream) {
        return IteratorExtensions.stream(IteratorExtensions.zipWithIndex(stream.iterator()));
    }

    /**
     * From a given stream of elements of type {@code T}, create a new stream
     * that associates each element with its mapped value.
     */
    public static <T, R> Stream<Map.Entry<T, R>> associate(
            final Stream<T> stream,
            final Function<? super T, ? extends R> mapper
    ) {
        return stream.map(t -> Map.entry(t, mapper.apply(t)));
    }
}
