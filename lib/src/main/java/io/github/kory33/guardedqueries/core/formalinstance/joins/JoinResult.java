package io.github.kory33.guardedqueries.core.formalinstance.joins;

import com.google.common.collect.ImmutableList;
import uk.ac.ox.cs.pdq.fol.Variable;

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
}
