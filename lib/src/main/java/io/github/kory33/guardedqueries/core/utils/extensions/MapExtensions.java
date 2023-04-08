package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

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

    public static <K, V> ImmutableMap<K, V> restrictToKeys(
            final Map<K, V> map,
            final Collection<K> keys
    ) {
        return ImmutableMapExtensions.consumeAndCopy(
                ImmutableSet.copyOf(keys).stream()
                        .flatMap(key -> map.containsKey(key)
                                ? Stream.of(Map.entry(key, map.get(key)))
                                : Stream.empty()
                        )
                        .iterator()
        );
    }

    public static <K, V> ImmutableBiMap<K, V> restrictToKeys(
            final ImmutableBiMap<K, V> map,
            final Collection<K> keys
    ) {
        // this does not lose any entry since a restriction of an injective map is again injective
        return ImmutableBiMap.copyOf(restrictToKeys((Map<K, V>) map, keys));
    }
}
