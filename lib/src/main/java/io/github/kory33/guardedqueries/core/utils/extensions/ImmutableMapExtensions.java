package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableMap;

import java.util.*;

public class ImmutableMapExtensions {
    private ImmutableMapExtensions() {
    }

    public static <K, V> ImmutableMap<K, V> consumeAndCopy(
            final Iterator<? extends Map.Entry<? extends K, ? extends V>> entries
    ) {
        final var builder = ImmutableMap.<K, V>builder();
        entries.forEachRemaining(entry -> builder.put(entry.getKey(), entry.getValue()));
        return builder.build();
    }

    /**
     * Returns a map that is the union of the given maps.
     * If the same key occurs in multiple maps, the value found in the last map is used.
     */
    @SafeVarargs
    public static <K, V> ImmutableMap<K, V> union(
            final Map<? extends K, ? extends V>... maps
    ) {
        // because ImmutableMap.Builder#putAll does not override existing entries
        // (and instead throws IllegalArgumentException), we need to keep track
        // the keys we have added so far

        // we reverse the input array so that we can "throw away" key-conflicting entries
        // that appear first in the input array
        final var inputMaps = new ArrayList<>(Arrays.asList(maps));
        Collections.reverse(inputMaps);

        final var keysWitnessed = new HashSet<K>();
        final var builder = ImmutableMap.<K, V>builder();
        for (final var map : inputMaps) {
            for (final var entry : map.entrySet()) {
                if (keysWitnessed.add(entry.getKey())) {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
        }

        return builder.build();
    }
}
