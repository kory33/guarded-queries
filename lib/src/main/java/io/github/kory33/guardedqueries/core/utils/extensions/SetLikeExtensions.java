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
     * Saturate a collection until generator function produces no additional elements when run on the set.
     * That is, the returned set is the smallest set {@code S} such that
     * <ol>
     *  <li>{@code S} contains all elements from the initial collection</li>
     *  <li>for every element {@code t} of {@code S}, {@code generator.apply(t)} is contained in {@code S}</li>
     * </ol>
     */
    public static <T> ImmutableSet<T> saturate(
            final Collection<? extends T> initialCollection,
            final Function<? super T, ? extends Collection<? extends T>> generator
    ) {
        final var hashSet = new HashSet<T>(initialCollection);
        while (true) {
            final var oldSize = hashSet.size();
            final ImmutableSet<T> generatedElements;
            {
                final var builder = ImmutableSet.<T>builder();
                // FIXME: we do not need to apply generator to the entire hashSet!
                //        only to the newly added elements suffice
                hashSet.forEach(e -> builder.addAll(generator.apply(e)));
                generatedElements = builder.build();
            }
            hashSet.addAll(generatedElements);

            if (hashSet.size() == oldSize) {
                // we have reached the fixpoint
                return ImmutableSet.copyOf(hashSet);
            }
        }
    }
}
