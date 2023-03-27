package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;

public class MapExtensions {
    /**
     * Computes a map that maps each value in {@code values} to its preimage in {@code map}.
     */
    public static <K, V> ImmutableMap<V, /* possibly empty, disjoint */ImmutableSet<K>> preimages(
            final Map<K, V> map,
            final Collection<V> values
    ) {
        final var valueSet = ImmutableSet.copyOf(values);

        return ImmutableMapExtensions.consumeAndCopy(
                valueSet.stream()
                        .map(value -> {
                            final var preimageIterator = map.entrySet().stream()
                                    .filter(entry -> entry.getValue().equals(value))
                                    .map(Map.Entry::getKey)
                                    .iterator();

                            return Map.entry(value, ImmutableSet.copyOf(preimageIterator));
                        })
                        .iterator()
        );
    }
}
