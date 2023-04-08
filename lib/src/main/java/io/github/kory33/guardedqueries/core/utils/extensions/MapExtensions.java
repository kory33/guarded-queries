package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

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

    public static <K, V1, V2> ImmutableMap<K, V2> composeWithFunction(
            final Map<K, V1> map,
            final Function<? super V1, ? extends V2> function
    ) {
        return ImmutableMapExtensions.consumeAndCopy(
                map.entrySet().stream()
                        .map(entry -> Map.entry(entry.getKey(), function.apply(entry.getValue())))
                        .iterator()
        );
    }
}
