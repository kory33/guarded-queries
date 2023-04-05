package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ConjunctiveQueryExtensions {
    private ConjunctiveQueryExtensions() {
    }

    /**
     * Computes a subquery of a given {@code ConjunctiveQuery} that includes only the atoms
     * satisfying a specified predicate.
     */
    public static ConjunctiveQuery filterAtoms(
            final ConjunctiveQuery conjunctiveQuery,
            final Predicate<? super Atom> atomPredicate
    ) {
        final var originalFreeVariables = ImmutableSet.copyOf(conjunctiveQuery.getFreeVariables());

        final var filteredAtoms = Arrays.stream(conjunctiveQuery.getAtoms())
                .filter(atomPredicate)
                .toArray(Atom[]::new);

        // variables in filteredAtoms that are free in the original conjunctiveQuery
        final var filteredFreeVariables = Arrays.stream(filteredAtoms)
                .flatMap(atom -> Arrays.stream(atom.getVariables()))
                .filter(originalFreeVariables::contains)
                .toArray(Variable[]::new);

        return ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms);
    }

    /**
     * Computes a subquery of a given Conjunctive Query that includes only the atoms
     * where all bound variables in the atom are present in a specified set of variables.
     * <p>
     * The set of variables in the returned subquery is a subset of {@code variables},
     * and a variable is bound in the returned subquery if and only if it is bound in {@code conjunctiveQuery}.
     * <p>
     * For example, if {@code conjunctiveQuery} is {@code ∃x,y,z. T(x,y,z) ∧ T(x,y,w) ∧ T(x,c,z)}
     * and {@code boundVariableSet} is {@code {x,y}}, then the returned subquery is
     * {@code ∃x,y. T(x,y,w)}.
     *
     * @param conjunctiveQuery The Conjunctive Query to compute the subquery from
     * @param variables        The filter of variables that should be included in the subquery.
     * @return The computed subquery.
     */
    public static ConjunctiveQuery strictlyInduceSubqueryByVariables(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        final var variableSet = ImmutableSet.<Variable>copyOf(variables);
        final var cqBoundVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());

        return filterAtoms(conjunctiveQuery, atom -> {
            // variables in the atom that are bound in the CQ
            final var atomBoundVariables = SetLikeExtensions.intersection(
                    Arrays.asList(atom.getVariables()),
                    cqBoundVariables
            );
            return variableSet.containsAll(atomBoundVariables);
        });
    }

    /**
     * Computes a subquery of a given Conjunctive Query that includes only the atoms
     * which have at least one bound variable in a specified set of variables.
     * <p>
     * For example, if {@code conjunctiveQuery} is {@code ∃x,y,z. T(x,y,w) ∧ T(x,c,z)}
     * and {@code boundVariableSet} is {@code {y}}, then the returned subquery is
     * {@code ∃x,y. T(x,y,w)}.
     */
    public static ConjunctiveQuery subqueryRelevantToVariables(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        final var variableSet = ImmutableSet.<Variable>copyOf(variables);
        final var cqBoundVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());

        return filterAtoms(conjunctiveQuery, atom -> {
            // variables in the atom that are bound in the CQ
            final var atomBoundVariables = SetLikeExtensions.intersection(
                    Arrays.asList(atom.getVariables()),
                    cqBoundVariables
            );
            return SetLikeExtensions.nontriviallyIntersects(atomBoundVariables, variableSet);
        });
    }

    /**
     * Variables in the strict neighbourhood of a given set of variables in the given CQ.
     * <p>
     * Given a conjunctive query {@code q} and a variable {@code x} appearing in {@code q},
     * {@code x} is said to be in the strict neighbourhood of a set {@code V} of variables if
     * <ol>
     *   <li>{@code x} is not an element of {@code V}, and</li>
     *   <li>{@code x} occurs in the subquery of {@code q} relevant to {@code V}.</li>
     * </ol>
     */
    public static ImmutableSet<Variable> neighbourhoodVariables(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        final var subquery = subqueryRelevantToVariables(conjunctiveQuery, variables);
        final var subqueryVariables = SetLikeExtensions.union(
                Arrays.asList(subquery.getBoundVariables()),
                Arrays.asList(subquery.getFreeVariables())
        );

        return SetLikeExtensions.difference(subqueryVariables, variables);
    }

    /**
     * Given a conjunctive query {@code conjunctiveQuery} and a set of variables {@code variables},
     * returns a stream of all {@code conjunctiveQuery}-connected components of {@code variables}.
     */
    public static Stream</* nonempty */ImmutableSet<Variable>> connectedComponents(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        if (variables.isEmpty()) {
            return Stream.empty();
        }

        final var unionFindTree = new SimpleUnionFindTree<Variable>(variables);
        for (final var atom : conjunctiveQuery.getAtoms()) {
            final var variablesToUnion = SetLikeExtensions.intersection(
                    ImmutableSet.copyOf(atom.getVariables()),
                    variables
            );
            unionFindTree.unionAll(variablesToUnion);
        }
        return unionFindTree.getEquivalenceClasses().stream();
    }

    /**
     * Given a conjunctive query {@code q} and a set {@code v} of variables in {@code q},
     * checks if {@code v} is connected in {@code q}.
     * <p>
     * A set of variables {@code V} is said to be connected in {@code q} if
     * there is at most one {@code q}-connected component of {@code V} in {@code q}.
     */
    public static boolean isConnected(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        return connectedComponents(conjunctiveQuery, variables).count() <= 1L;
    }

    public static ImmutableSet<Constant> constantsIn(
            final ConjunctiveQuery conjunctiveQuery
    ) {
        return ImmutableSet.copyOf(
                StreamExtensions
                        .filterSubtype(Arrays.stream(conjunctiveQuery.getTerms()), Constant.class)
                        .iterator()
        );
    }
}
