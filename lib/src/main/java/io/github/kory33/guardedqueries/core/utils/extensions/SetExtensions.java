package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SetExtensions {
    private SetExtensions() {
    }

    /**
     * Intersection of two sets.
     */
    public static <T> ImmutableSet<T> intersection(final Set<? extends T> set1, final Set<? extends T> set2) {
        return ImmutableSet.copyOf(set1.stream().filter(set2::contains).iterator());
    }
}
