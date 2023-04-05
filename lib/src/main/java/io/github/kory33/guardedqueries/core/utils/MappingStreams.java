package io.github.kory33.guardedqueries.core.utils;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions;

import java.util.Collection;
import java.util.stream.Stream;

public class MappingStreams {
    private MappingStreams() {
    }

    public static <K, V> Stream<ImmutableMap<K, V>> allTotalFunctionsBetween(
            final Collection<K> domain,
            final Collection<V> range
    ) {
        throw new RuntimeException("Not implemented yet");
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
