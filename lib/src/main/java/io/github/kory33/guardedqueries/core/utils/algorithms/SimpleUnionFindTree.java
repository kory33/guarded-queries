package io.github.kory33.guardedqueries.core.utils.algorithms;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class SimpleUnionFindTree<V /* nonnull, hashable, eq-comparable */> {
    // invariants:
    //  - referenceTowardsRepresentative.keySet() intersection representatives is empty
    //  - referenceTowardsRepresentative.keySet() union representatives is constant after every operation
    private final HashMap<V, V> referenceTowardsRepresentative;
    private final HashSet<V> representatives;

    public SimpleUnionFindTree(final Collection<? extends V> values) {
        this.representatives = new HashSet<>(values);
        this.referenceTowardsRepresentative = new HashMap<>();
    }

    public V representativeOfClassOf(final V value) {
        V current = value;
        while (true) {
            if (representatives.contains(current)) {
                return current;
            } else if (referenceTowardsRepresentative.containsKey(current)) {
                current = referenceTowardsRepresentative.get(current);
            } else {
                throw new IllegalArgumentException("Unrecognized by the UF tree: " + value.toString());
            }
        }
    }

    public void unionTwo(final V v1, final V v2) {
        final var v1Representative = representativeOfClassOf(v1);
        final var v2Representative = representativeOfClassOf(v2);

        if (v1Representative != v2Representative) {
            // let v2Representative point to v1Representative
            referenceTowardsRepresentative.put(v2Representative, v1Representative);
            representatives.remove(v2Representative);
        }
    }

    public void unionAll(final Collection<V> values) {
        final var iterator = values.iterator();
        if (!iterator.hasNext()) {
            return;
        }
        final var first = iterator.next();
        iterator.forEachRemaining(value -> unionTwo(first, value));
    }

    public ImmutableSet<ImmutableSet<V>> getEquivalenceClasses() {
        final HashMap<V /* representative */, HashSet<V> /* partial equivalence class */> equivClasses = new HashMap<>();
        for (final var representative : this.representatives) {
            final var freshClass = new HashSet<V>();
            freshClass.add(representative);
            equivClasses.put(representative, freshClass);
        }

        for (final var nonRepresentative : this.referenceTowardsRepresentative.keySet()) {
            equivClasses.get(this.representativeOfClassOf(nonRepresentative)).add(nonRepresentative);
        }

        return ImmutableSet.copyOf(
                equivClasses.values()
                        .stream()
                        .map(ImmutableSet::copyOf)
                        .iterator()
        );
    }
}
