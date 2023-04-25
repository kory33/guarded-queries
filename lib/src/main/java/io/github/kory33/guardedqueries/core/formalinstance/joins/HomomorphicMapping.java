package io.github.kory33.guardedqueries.core.formalinstance.joins;

import com.google.common.collect.ImmutableList;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.function.Function;

/**
 * A mapping from variables to terms, which is part of a homomorphism mapping a query to an instance.
 * <p>
 * {@code variableOrdering} must be an ordered list of variables to be mapped, and this
 * must not contain any duplicate variables (no runtime check is enforced for this).
 * The {@code i}-th element of {@code orderedMapping} specifies to which term the {@code i}-th
 * variable in {@code variableOrdering} is mapped.
 * <p>
 * For instance, suppose that {@code variableOrdering = [x, y, z]} and {@code orderedMapping = [a, a, b]}.
 * Then the mapping this object represents is {@code x -> a, y -> a, z -> b}.
 */
public record HomomorphicMapping<Term>(
        ImmutableList<Variable> variableOrdering,
        ImmutableList<Term> orderedMapping
) implements Function<Variable, Term> {
    public HomomorphicMapping {
        if (variableOrdering.size() != orderedMapping.size()) {
            throw new IllegalArgumentException("variableOrdering and orderedMapping must have the same size");
        }
    }

    /**
     * Returns the term to which the given variable is mapped.
     *
     * @param variable a variable in {@code variableOrdering}
     * @return the term to which the given variable is mapped
     * @throws IllegalArgumentException if the given variable is not in {@code variableOrdering}
     */
    public Term apply(Variable variable) {
        final var index = variableOrdering.indexOf(variable);
        if (index == -1) {
            throw new IllegalArgumentException("variable is not in variableOrdering");
        }
        return orderedMapping.get(index);
    }
}
