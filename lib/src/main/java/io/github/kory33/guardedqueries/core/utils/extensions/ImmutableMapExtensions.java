package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

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
            final ImmutableMap<? extends K, ? extends V>... maps
    ) {
        final var builder = ImmutableMap.<K, V>builder();
        for (final var map : maps) {
            builder.putAll(map);
        }
        return builder.build();
    }
}
