package io.github.kory33.guardedqueries.core.formalinstance.joins;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Map;
import java.util.function.Function;

/**
 * A class of objects representing the result of a join operation.
 * <p>
 * We say that a {@code JoinResult} is well-formed if:
 * <ol>
 *     <li>the {@code variableOrdering} is a list of distinct variables</li>
 *     <li>each list in {@code orderedMapping} has the same length as {@code variableOrdering}</li>
 *     <li>{@code orderedMapping} is a list opf distinct join result tuples</li>
 * </ol>
 * <p>
 * For example, given a query {@code Q(x, y), R(y, z)}, a {@code JoinResult} of the form
 * <ol>
 *     <li>{@code variableOrdering = [x, y, z]}</li>
 *     <li>{@code orderedMapping = [[a, b, c], [a, c, d]]}</li>
 * </ol>
 * is a well-formed answer to the query, insinuating that {@code Q(a, b), R(b, c)} and
 * {@code Q(a, c), R(c, d)} are the only answers to the query.
 *
 * @param <Term> type of values (typically constants) that appear in the input instance of the join operation
 */
public class JoinResult<Term> {
    // invariant: a single instance of ImmutableList<Variable> variableOrdering is shared among
    //            all HomomorphicMapping objects
    public final ImmutableList<HomomorphicMapping<Term>> allHomomorphisms;

    public JoinResult(
            final ImmutableList<Variable> variableOrdering,
            final ImmutableList<ImmutableList<Term>> orderedMappingsOfAllHomomorphisms
    ) {
        this.allHomomorphisms = ImmutableList.copyOf(
                orderedMappingsOfAllHomomorphisms.stream()
                        .map(homomorphism -> new HomomorphicMapping<>(variableOrdering, homomorphism))
                        .iterator()
        );
    }

    /**
     * Materialize the given atom by replacing the variables in the atom with the values in this result.
     * <p>
     * The returned list has the same length as {@code orderedMapping}.
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
                    return homomorphism.apply(variable);
                } else {
                    throw new IllegalArgumentException("Term " + term + " is neither constant nor variable");
                }
            });
        });

        return result.build();
    }

    /**
     * Extend the join result by adjoining a constant homomorphism.
     * <p>
     * For instance, suppose that this join result is obtained as an answer to a query {@code Q(x, y), R(y, z)}
     * and has the following data:
     * <ol>
     *     <li>{@code allHomomorphisms = [{x -> a, y -> b, z -> c}, {x -> a, y -> c, z -> d}]}</li>
     * </ol>
     * We can "extend" these results by adjoining a constant homomorphism {@code {w -> e}}.
     * The result of such operation is:
     * <ol>
     *     <li>{@code allHomomorphisms = [{x -> a, y -> b, z -> c, w -> e}, {x -> a, y -> c, z -> d, w -> e}]}</li>
     * </ol>
     *
     * @param constantHomomorphism The homomorphism with which the join result is to be extended
     * @return the extended join result
     * @throws IllegalArgumentException if the given homomorphism maps a variable in {@code variableOrdering}
     */
    public JoinResult<Term> extendWithConstantHomomorphism(
            final Map<Variable, Term> constantHomomorphism
    ) {
        if (allHomomorphisms.isEmpty()) {
            // then this join result contains no information and there is nothing to extend
            return this;
        }

        final var variableOrdering = allHomomorphisms.get(0).variableOrdering();

        if (variableOrdering.stream().anyMatch(constantHomomorphism::containsKey)) {
            throw new IllegalArgumentException("The given constant homomorphism has a conflicting variable mapping");
        }

        final var extensionVariableOrdering = ImmutableList.copyOf(constantHomomorphism.keySet());
        final var extensionMapping = ImmutableList.copyOf(
                extensionVariableOrdering.stream().map(constantHomomorphism::get).iterator()
        );

        final var extendedVariableOrdering = ImmutableList.<Variable>builder()
                .addAll(variableOrdering)
                .addAll(extensionVariableOrdering)
                .build();

        final var extendedHomomorphisms = allHomomorphisms.stream().map(homomorphism ->
                ImmutableList.<Term>builder()
                        .addAll(homomorphism.orderedMapping())
                        .addAll(extensionMapping)
                        .build()
        );

        return new JoinResult<>(extendedVariableOrdering, ImmutableList.copyOf(extendedHomomorphisms.iterator()));
    }
}
