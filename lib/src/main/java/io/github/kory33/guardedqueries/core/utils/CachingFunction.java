package io.github.kory33.guardedqueries.core.utils;


import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.function.Function;

public final class CachingFunction<T, R> implements Function<T, R> {
    private final HashMap<T, R> cache;
    private final Function<T, R> function;

    public CachingFunction(final Function<T, R> function) {
        this.cache = new HashMap<>();
        this.function = function;
    }

    @Override
    public R apply(T t) {
        if (cache.containsKey(t)) {
            return cache.get(t);
        } else {
            R result = function.apply(t);
            cache.put(t, result);
            return result;
        }
    }

    public ImmutableSet<T> computedDomain() {
        return ImmutableSet.copyOf(cache.keySet());
    }

    public ImmutableSet<R> computedRange() {
        return ImmutableSet.copyOf(cache.values());
    }
}
