package io.github.kory33.guardedqueries.core.utils.extensions;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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

    public static <T> Iterable<T> asIterable(final Stream<T> stream) {
        return stream::iterator;
    }

    public static <T1, T2 extends T1> Stream<T2> filterSubtype(final Stream<T1> stream, final Class<T2> clazz) {
        return stream.filter(clazz::isInstance).map(clazz::cast);
    }

    public static <State, Output> Stream<Output> unfold(
            final State initialState,
            final Function<? super State, Optional<? extends Pair<? extends Output, ? extends State>>> yieldNext
    ) {
        final Iterator<Output> iterator = new Iterator<Output>() {
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
            Optional<? extends Pair<? extends Output, ? extends State>> currentOutputStatePair = yieldNext.apply(initialState);

            @Override
            public boolean hasNext() {
                return currentOutputStatePair.isPresent();
            }

            @Override
            public Output next() {
                if (currentOutputStatePair.isEmpty()) {
                    throw new NoSuchElementException();
                }

                final Output output = currentOutputStatePair.get().getLeft();
                currentOutputStatePair = yieldNext.apply(currentOutputStatePair.get().getRight());
                return output;
            }
        };

        return IteratorExtensions.stream(iterator);
    }
}
