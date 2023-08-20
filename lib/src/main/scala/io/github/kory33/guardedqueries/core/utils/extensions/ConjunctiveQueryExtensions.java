package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ConjunctiveQueryExtensions {
    private ConjunctiveQueryExtensions() {
    }

    /**
     * Computes a subquery of a given {@code ConjunctiveQuery} that includes only the atoms
     * satisfying a specified predicate.
     * <p>
     * Since {@code ConjunctiveQuery} cannot be empty, an empty {@code Optional} is returned
     * if the predicate is not satisfied by any atom in the given {@code ConjunctiveQuery}.
     */
    public static Optional<ConjunctiveQuery> filterAtoms(
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

        if (filteredAtoms.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms));
        }
    }

    /**
     * Computes a subquery of a given Conjunctive Query that includes only the atoms
     * that contains at least one bound variable and all bound variables in the atom
     * are present in a specified set of variables.
     * <p>
     * The set of variables in the returned subquery is a subset of {@code variables},
     * and a variable is bound in the returned subquery if and only if it is bound in {@code conjunctiveQuery}.
     * <p>
     * For example, if {@code conjunctiveQuery} is {@code ∃x,y,z. T(x,y,z) ∧ T(x,y,w) ∧ T(x,c,z)}
     * and {@code boundVariableSet} is {@code {x,y}}, then the returned subquery is
     * {@code ∃x,y. T(x,y,w)}.
     * <p>
     * If no atom in the given {@code ConjunctiveQuery} has variable set entirely contained in
     * {@code variables}, an empty {@code Optional} is returned.
     *
     * @param conjunctiveQuery The Conjunctive Query to compute the subquery from
     * @param variables        The filter of variables that should be included in the subquery.
     * @return The computed subquery.
     */
    public static Optional<ConjunctiveQuery> strictlyInduceSubqueryByVariables(
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
            return variableSet.containsAll(atomBoundVariables) && atomBoundVariables.size() > 0;
        });
    }

    /**
     * Computes a subquery of a given Conjunctive Query that includes only the atoms
     * which have at least one bound variable in a specified set of variables.
     * <p>
     * For example, if {@code conjunctiveQuery} is {@code ∃x,y,z. T(x,y,w) ∧ T(x,c,z)}
     * and {@code boundVariableSet} is {@code {y}}, then the returned subquery is
     * {@code ∃x,y. T(x,y,w)}.
     * <p>
     * If no atom in the given {@code ConjunctiveQuery} has variable set intersecting with
     * {@code variables}, an empty {@code Optional} is returned.
     */
    public static Optional<ConjunctiveQuery> subqueryRelevantToVariables(
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
        if (subquery.isEmpty()) {
            return ImmutableSet.of();
        }

        return SetLikeExtensions.difference(variablesIn(subquery.get()), variables);
    }

    /**
     * Given a conjunctive query {@code conjunctiveQuery} and a set {@code boundVariables} of variables
     * bound in {@code conjunctiveQuery}, returns a stream of all {@code conjunctiveQuery}-connected
     * components of {@code variables}.
     */
    public static Stream</* nonempty */ImmutableSet<Variable>> connectedComponents(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> boundVariables
    ) {
        if (boundVariables.isEmpty()) {
            return Stream.empty();
        }

        for (final var variable : boundVariables) {
            if (!Arrays.asList(conjunctiveQuery.getBoundVariables()).contains(variable)) {
                throw new IllegalArgumentException("Variable " + variable + " is not bound in the given CQ");
            }
        }

        final var unionFindTree = new SimpleUnionFindTree<Variable>(boundVariables);
        for (final var atom : conjunctiveQuery.getAtoms()) {
            final var variablesToUnion = SetLikeExtensions.intersection(
                    ImmutableSet.copyOf(atom.getVariables()),
                    boundVariables
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

    public static ImmutableSet<Variable> variablesIn(
            final ConjunctiveQuery conjunctiveQuery
    ) {
        return SetLikeExtensions.union(
                Arrays.asList(conjunctiveQuery.getBoundVariables()),
                Arrays.asList(conjunctiveQuery.getFreeVariables())
        );
    }
}
