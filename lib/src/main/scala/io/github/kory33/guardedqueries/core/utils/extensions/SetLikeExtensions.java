package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SetLikeExtensions {
    private SetLikeExtensions() {
    }

    /**
     * Union of elements from two collections.
     */
    public static <T> ImmutableSet<T> union(
            final Collection<? extends T> collection1,
            final Collection<? extends T> collection2
    ) {
        return ImmutableSet.<T>builder()
                .addAll(collection1)
                .addAll(collection2)
                .build();
    }

    /**
     * Intersection of elements from two collections.
     */
    public static <T> ImmutableSet<T> intersection(
            final Collection<? extends T> collection1,
            final Collection<? extends T> collection2
    ) {
        final var set2 = ImmutableSet.copyOf(collection2);
        return ImmutableSet.copyOf(collection1.stream().filter(set2::contains).iterator());
    }

    /**
     * Check if two collections have any common elements.
     */
    public static boolean nontriviallyIntersects(
            final Collection<?> collection1,
            final Collection<?> collection2
    ) {
        final var set2 = ImmutableSet.copyOf(collection2);
        return collection1.stream().anyMatch(set2::contains);
    }

    /**
     * Check if two collections have no common elements.
     */
    public static boolean disjoint(
            final Collection<?> collection1,
            final Collection<?> collection2
    ) {
        return !nontriviallyIntersects(collection1, collection2);
    }

    /**
     * Set difference of elements from two collections.
     */
    public static <T> ImmutableSet<T> difference(
            final Collection<? extends T> collection1,
            final Collection<? extends T> collection2
    ) {
        final var set2 = ImmutableSet.copyOf(collection2);
        return ImmutableSet.copyOf(collection1.stream().filter(e -> !set2.contains(e)).iterator());
    }

    /**
     * Powerset of a set of elements from the given collection, lazily streamed.
     */
    public static <T> Stream<ImmutableSet<T>> powerset(final Collection<? extends T> collection) {
        // deduplicated ArrayList of elements
        final var arrayList = new ArrayList<>(ImmutableSet.copyOf(collection));
        final var setSize = arrayList.size();

        // every non-negative BigInteger less than this value represents a unique subset of the given collection
        final var upperLimit = BigInteger.ONE.shiftLeft(setSize);

        return StreamExtensions.unfold(BigInteger.ZERO, currentIndex -> {
            // currentIndex < upperLimit
            if (currentIndex.compareTo(upperLimit) < 0) {
                final var subset = ImmutableSet.<T>copyOf(
                        IntStream.range(0, setSize)
                                .filter(currentIndex::testBit)
                                .mapToObj(arrayList::get)
                                .iterator()
                );
                return Optional.of(Pair.of(subset, currentIndex.add(BigInteger.ONE)));
            } else {
                return Optional.empty();
            }
        });
    }

    /**
     * Saturate a collection of elements of type {@code T} by repeatedly applying a
     * generator function {@code generator} that generates a collection of elements
     * of type {@code T} from values of type {@code T}.
     * <p>
     * More precisely, the returned set is the smallest set {@code S} such that
     * <ol>
     *  <li>{@code S} contains all elements from the initial collection</li>
     *  <li>for every element {@code t} of {@code S}, {@code generator.apply(t)} is contained in {@code S}</li>
     * </ol>
     */
    public static <T> ImmutableSet<T> generateFromElementsUntilFixpoint(
            final Collection<? extends T> initialCollection,
            final Function<? super T, ? extends Collection<? extends T>> generator
    ) {
        final var hashSet = new HashSet<T>(initialCollection);

        var elementsAddedInPreviousIteration = ImmutableSet.copyOf(hashSet);
        while (!elementsAddedInPreviousIteration.isEmpty()) {
            final ImmutableSet<T> newlyGeneratedElements;
            {
                final var builder = ImmutableSet.<T>builder();

                // assuming that the generator function is pure,
                // it is only meaningful to generate new elements
                // from elements that have been newly added to the set
                // in the previous iteration
                elementsAddedInPreviousIteration.forEach(newElementToConsider -> {
                    for (final var generatedElement : generator.apply(newElementToConsider)) {
                        // we only add elements that are not already in the set
                        if (!hashSet.contains(generatedElement)) {
                            builder.add(generatedElement);
                        }
                    }
                });

                newlyGeneratedElements = builder.build();
            }

            hashSet.addAll(newlyGeneratedElements);
            elementsAddedInPreviousIteration = newlyGeneratedElements;
        }

        return ImmutableSet.copyOf(hashSet);
    }

    /**
     * Saturate a collection of elements of type {@code T} by repeatedly applying a
     * generator function {@code generator} that generates a collection of elements
     * of type {@code T} from a collection of values of type {@code T}.
     * <p>
     * More precisely, the returned set is the smallest set {@code S} such that
     * <ol>
     *  <li>{@code S} contains all elements from the initial collection</li>
     *  <li>{@code generator.apply(S)} is contained in {@code S}</li>
     * </ol>
     */
    public static <T> ImmutableSet<T> generateFromSetUntilFixpoint(
            final Collection<? extends T> initialCollection,
            final Function<? super ImmutableSet<T>, ? extends Collection<? extends T>> generator
    ) {
        final var hashSet = new HashSet<T>(initialCollection);

        while (true) {
            final var elementsGeneratedSoFar = ImmutableSet.copyOf(hashSet);
            final var elementsGeneratedInThisIteration =
                    ImmutableSet.<T>copyOf(generator.apply(elementsGeneratedSoFar));

            if (hashSet.containsAll(elementsGeneratedInThisIteration)) {
                // we have reached the least fixpoint above initialCollection
                return ImmutableSet.copyOf(hashSet);
            } else {
                hashSet.addAll(elementsGeneratedInThisIteration);
            }
        }
    }
}
