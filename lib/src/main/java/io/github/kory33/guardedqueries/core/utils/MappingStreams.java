package io.github.kory33.guardedqueries.core.utils;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.ImmutableMapExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MappingStreams {
    private MappingStreams() {
    }

    public static <K, V> Stream<ImmutableMap<K, V>> allTotalFunctionsBetween(
            final Collection<K> domain,
            final Collection<V> range
    ) {
        final var orderedDomain = ImmutableList.copyOf(ImmutableSet.copyOf(domain));
        final var orderedRange = ImmutableList.copyOf(ImmutableSet.copyOf(range));
        final var rangeSize = orderedRange.size();

        /*
         * An internal state representing a mapping between domain and range.
         *
         * A value of this class is essentially an int array rangeElementIndices of length orderedDomain.size(),
         * representing a mapping that sends orderedDomain.get(i) to orderedRange.get(rangeElementIndices[i]).
         */
        class RangeIndexArray {
            final int[] rangeElementIndices = new int[orderedDomain.size()];
            // boolean indicating whether we have emitted the last map
            // which corresponds to all entries in rangeElementIndices being rangeSize - 1
            private boolean hasReachedEndAndIncrementAttempted = false;

            /**
             * Increment index array. For example, if the array is [5, 4, 2] and rangeSize is 6,
             * we increment the array to [0, 5, 2] (increment the leftmost index that is not at rangeSize - 1,
             * and clear all indices to the left). If all indices are at the maximum value,
             * no indices are modified and false is returned.
             */
            public void increment() {
                for (int i = 0; i < rangeElementIndices.length; i++) {
                    if (rangeElementIndices[i] < rangeSize - 1) {
                        rangeElementIndices[i]++;
                        for (int j = i - 1; j >= 0; j--) {
                            rangeElementIndices[j] = 0;
                        }
                        return;
                    }
                }
                hasReachedEndAndIncrementAttempted = true;
            }

            public boolean hasReachedEndAndIncrementAttempted() {
                return hasReachedEndAndIncrementAttempted;
            }

            public ImmutableMap<K, V> toMap() {
                return ImmutableMapExtensions.consumeAndCopy(IntStream
                        .range(0, rangeElementIndices.length)
                        .mapToObj(i -> Pair.of(orderedDomain.get(i), orderedRange.get(rangeElementIndices[i])))
                        .iterator());
            }
        }

        return StreamExtensions.unfoldMutable(
                new RangeIndexArray(),
                indexArray -> {
                    if (indexArray.hasReachedEndAndIncrementAttempted()) {
                        return Optional.empty();
                    } else {
                        final var output = indexArray.toMap();
                        indexArray.increment();
                        return Optional.of(output);
                    }
                }
        );
    }

    public static <K, V> Stream<ImmutableMap<K, V>> allPartialFunctionsBetween(
            final Collection<K> domain,
            final Collection<V> range
    ) {
        return SetLikeExtensions.powerset(domain).flatMap(domainSubset -> allTotalFunctionsBetween(domainSubset, range));
    }

    public static <K, V> Stream<ImmutableBiMap<K, V>> allInjectiveTotalFunctionsBetween(
            final Collection<K> domain,
            final Collection<V> range
    ) {
        final var orderedDomain = ImmutableList.copyOf(ImmutableSet.copyOf(domain));
        final var orderedRange = ImmutableList.copyOf(ImmutableSet.copyOf(range));
        final var rangeSize = orderedRange.size();

        if (orderedDomain.size() > rangeSize) {
            return Stream.empty();
        }

        /*
         * An internal state representing an injective mapping between domain and range.
         *
         * A value of this class is essentially an int array rangeElementIndices of length orderedDomain.size(),
         * and produces injective mappings in the lexicographical order.
         */
        class RangeIndexArray {
            // [0,1,...,orderedDomain.size()-1] is the first injective mapping in the lexicographical order
            private final int[] rangeElementIndices = IntStream.range(0, orderedDomain.size()).toArray();

            // boolean indicating whether we have called increment() after
            // reaching the maximum rangeElementIndices, which is
            // [rangeSize-1, rangeSize-2, ..., rangeSize - orderedDomain.size()]
            private boolean hasReachedEndAndIncrementAttempted = false;

            /**
             * Increment index array.
             * <p>
             * We scan the index array from the end, and we try to increment the entry (while maintaining injectivity)
             * as early as possible. If we cannot increment a particular entry, we drop it (conceptually, without
             * actually resizing the array) and try to increment the previous entry. After having incremented
             * an entry, we clear all entries to the right of it, and then we fill the cleared entries with
             * the smallest increasing sequence of integers that is not already used by previous entries.
             * <p>
             * For example, if the array is [0,4,5] and rangeSize is 6,
             * we first look at 5 and try to increment it. Since 5 is not at the maximum value, we
             * now consider the array to be [0,4] and continue the process. Since 4 can be incremented,
             * we increment it to 5. We now have [0,5], so we fill the cleared entries with the smallest
             * increasing sequence, which is [1], so we end up with [0,5,1].
             */
            public void increment() {
                final HashSet<Integer> availableIndices;
                {
                    final var usedIndices = Arrays.stream(rangeElementIndices).boxed()
                            .collect(Collectors.toCollection(HashSet::new));
                    availableIndices = IntStream.range(0, rangeSize).boxed()
                            .filter(i -> !usedIndices.contains(i))
                            .collect(Collectors.toCollection(HashSet::new));
                }
                for (int i = rangeElementIndices.length - 1; i >= 0; i--) {
                    final var oldEntry = rangeElementIndices[i];
                    final var incrementableTo = availableIndices
                            .stream()
                            .filter(index -> index > oldEntry)
                            .findFirst();

                    if (incrementableTo.isPresent()) {
                        final var newEntry = incrementableTo.get();
                        rangeElementIndices[i] = newEntry;
                        availableIndices.add(oldEntry);
                        availableIndices.remove(newEntry);

                        final var sortedAvailableIndices = new ArrayList<>(availableIndices);
                        sortedAvailableIndices.sort(Integer::compareTo);

                        for (int j = i + 1; j < rangeElementIndices.length; j++) {
                            rangeElementIndices[j] = sortedAvailableIndices.get(j - i - 1);
                        }

                        return;
                    } else {
                        // we "drop" the entry and conceptually shorten the array
                        availableIndices.add(oldEntry);
                    }
                }
                hasReachedEndAndIncrementAttempted = true;
            }

            public boolean hasReachedEndAndIncrementAttempted() {
                return hasReachedEndAndIncrementAttempted;
            }

            public ImmutableBiMap<K, V> toMap() {
                final var builder = ImmutableBiMap.<K, V>builder();
                IntStream
                        .range(0, rangeElementIndices.length)
                        .mapToObj(i -> Pair.of(orderedDomain.get(i), orderedRange.get(rangeElementIndices[i])))
                        .forEach(builder::put);
                return builder.build();
            }
        }

        return StreamExtensions.unfoldMutable(
                new RangeIndexArray(),
                indexArray -> {
                    if (indexArray.hasReachedEndAndIncrementAttempted()) {
                        return Optional.empty();
                    } else {
                        final var output = indexArray.toMap();
                        indexArray.increment();
                        return Optional.of(output);
                    }
                }
        );
    }
}
