package io.github.kory33.guardedqueries.core.utils;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.ImmutableMapExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Optional;
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
            // boolean indicating whether all entries in rangeElementIndices are rangeSize - 1
            boolean hasReachedEnd = false;

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
                hasReachedEnd = true;
            }

            public boolean hasReachedEnd() {
                return hasReachedEnd;
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
                    if (indexArray.hasReachedEnd) {
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
        throw new RuntimeException("Not implemented yet");
    }
}
