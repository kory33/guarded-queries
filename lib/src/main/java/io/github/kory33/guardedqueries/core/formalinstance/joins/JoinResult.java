package io.github.kory33.guardedqueries.core.formalinstance.joins;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.function.Function;

/**
 * A class of objects representing the result of a join operation.
 * <p>
 * We say that a {@code JoinResult} is well-formed if:
 * <ol>
 *     <li>the {@code variableOrdering} is a list of distinct variables</li>
 *     <li>each list in {@code allHomomorphisms} has the same length as {@code variableOrdering}</li>
 *     <li>{@code allHomomorphisms} is a list opf distinct join result tuples</li>
 * </ol>
 * <p>
 * For example, given a query {@code Q(x, y), R(y, z)}, a {@code JoinResult} of the form
 * <ol>
 *     <li>{@code variableOrdering = [x, y, z]}</li>
 *     <li>{@code allHomomorphisms = [[a, b, c], [a, c, d]]}</li>
 * </ol>
 * is a well-formed answer to the query, insinuating that {@code Q(a, b), R(b, c)} and
 * {@code Q(a, c), R(c, d)} are the only answers to the query.
 *
 * @param <Term> type of values (typically constants) that appear in the input instance of the join operation
 */
public record JoinResult<Term>(
        ImmutableList<Variable> variableOrdering,
        ImmutableList<ImmutableList<Term>> allHomomorphisms
) {
    /**
     * Materialize the given atom by replacing the variables in the atom with the values in this result.
     * <p>
     * The returned list has the same length as {@code allHomomorphisms}.
     *
     * @param atomWhoseVariablesAreInThisResult a function-free atom whose variables are in {@code variableOrdering}
     * @param constantInclusion                 a function that maps a constant in the input instance to a term
     * @return a list of formal facts that are the result of materializing the given atom
     */
    public ImmutableList<FormalFact<Term>> materializeFunctionFreeAtom(
            final Atom atomWhoseVariablesAreInThisResult,
            final Function<Constant, Term> constantInclusion
    ) {
        final var inputAtomAsFormalFact = FormalFact.fromAtom(atomWhoseVariablesAreInThisResult);
        final var result = ImmutableList.<FormalFact<Term>>builder();

        allHomomorphisms.forEach(homomorphism -> {
            inputAtomAsFormalFact.map(term -> {
                if (term instanceof Constant constant) {
                    return constantInclusion.apply(constant);
                } else if (term instanceof Variable variable) {
                    return homomorphism.get(variableOrdering.indexOf(variable));
                } else {
                    throw new IllegalArgumentException("Term " + term + " is neither constant nor variable");
                }
            });
        });

        return result.build();
    }
}
