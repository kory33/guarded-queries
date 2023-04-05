package io.github.kory33.guardedqueries.core.utils.extensions;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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

    /**
     * Create a stream that yields elements of type {@code Output} by mutating a mutable state of type {@code MutableState}.
     * The stream terminates when the function {@code mutateAndYieldNext} returns an empty optional.
     * <p>
     * The function {@code mutateAndYieldNext} should be a stateless {@code Function} object.
     */
    public static <MutableState, Output> Stream<Output> unfoldMutable(
            final MutableState mutableState,
            final Function<? super MutableState, Optional<Output>> mutateAndYieldNext
    ) {
        final Iterator<Output> iterator = new Iterator<Output>() {
            private final MutableState nextState;
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
            private Optional<Output> nextOutput;

            {
                nextOutput = mutateAndYieldNext.apply(mutableState);
                nextState = mutableState;
            }

            @Override
            public boolean hasNext() {
                return nextOutput.isPresent();
            }

            @Override
            public Output next() {
                @SuppressWarnings("OptionalGetWithoutIsPresent") final var output = nextOutput.get();
                nextOutput = mutateAndYieldNext.apply(nextState);
                return output;
            }
        };

        return IteratorExtensions.stream(iterator);
    }

    /**
     * Create a stream that yields elements of type by repeatedly applying a function {@code yieldNext}
     * to a state of type {@code State}. The function {@code yieldNext} should be a stateless {@code Function} object.
     */
    public static <State, Output> Stream<Output> unfold(
            final State initialState,
            final Function<? super State, Optional<? extends Pair<? extends Output, ? extends State>>> yieldNext
    ) {
        AtomicReference<State> stateCell = new AtomicReference<>(initialState);
        return unfoldMutable(stateCell, cell -> {
            final var nextPair = yieldNext.apply(cell.get());
            if (nextPair.isPresent()) {
                cell.set(nextPair.get().getRight());
                return Optional.of(nextPair.get().getLeft());
            } else {
                return Optional.empty();
            }
        });
    }

}
