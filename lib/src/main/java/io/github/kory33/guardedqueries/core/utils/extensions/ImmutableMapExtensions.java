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
}
