package io.github.kory33.guardedqueries.core.utils.extensions;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IteratorExtensions {
    private IteratorExtensions() {
    }

    /**
     * Zip two iterators together.
     */
    public static <L, R> Iterator<Pair<L, R>> zip(final Iterator<L> leftIterator, final Iterator<R> rightIterator) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return leftIterator.hasNext() && rightIterator.hasNext();
            }

            @Override
            public Pair<L, R> next() {
                return Pair.of(leftIterator.next(), rightIterator.next());
            }
        };
    }

    /**
     * Zip the iterator together with an infinite Long stream starting from 0.
     */
    public static <T> Iterator<Pair<T, Long>> zipWithIndex(final Iterator<T> iterator) {
        return zip(iterator, LongStream.range(0, Long.MAX_VALUE).iterator());
    }

    /**
     * Convert an iterator to a stream.
     */
    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        final Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
