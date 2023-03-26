package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

public class ConjunctiveQueryExtensions {
    private ConjunctiveQueryExtensions() {
    }

    /**
     * Computes a subquery of a given Conjunctive Query that includes only the atoms
     * where all variables bound in the atom are present in a specified set of variables.
     * <p>
     * The set of bound (resp. free) variables in the returned subquery is the
     * subset of {@code boundVariableSet} (resp. the free variables in {@code conjunctiveQuery}).
     *
     * @param conjunctiveQuery The Conjunctive Query to compute the subquery from
     * @param boundVariables   The set of variables that should be included in the subquery.
     * @return The computed subquery.
     * @throws IllegalArgumentException if {@code boundVariableSet} is not a subset
     *                                  of the bound variables in {@code conjunctiveQuery}.
     */
    public static ConjunctiveQuery strictlyInduceSubqueryByBoundVariables(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> boundVariables
    ) {
        final var boundVariableSet = ImmutableSet.<Variable>copyOf(boundVariables);
        final var cqBoundVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());

        // we would like to eliminate cases such as conjunctiveQuery: ∃x,y,z. T(x,y,z), boundVariableSet: {x,y,w}
        if (!cqBoundVariables.containsAll(boundVariables)) {
            throw new IllegalArgumentException(
                    "The given set of bound variables is not a subset of the bound variables of the given CQ."
            );
        }

        final var filteredAtoms = Arrays.stream(conjunctiveQuery.getAtoms())
                .filter(atom -> {
                    // variables in the atom that are bound in the CQ
                    final var atomBoundVariables = SetExtensions.intersection(
                            ImmutableSet.copyOf(atom.getVariables()),
                            cqBoundVariables
                    );
                    return boundVariableSet.containsAll(atomBoundVariables);
                })
                .toArray(Atom[]::new);

        // variables in filteredAtoms that are free in the CQ
        final var filteredFreeVariables = Arrays.stream(filteredAtoms)
                .flatMap(atom -> Arrays.stream(atom.getVariables()))
                .filter(variable -> !cqBoundVariables.contains(variable))
                .toArray(Variable[]::new);

        return ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms);
    }

    /**
     * Variables in the strict neighbourhood of a given set of bound variables in the given CQ.
     * <p>
     * Given a conjunctive query {@code q} and a variable {@code x} appearing (either bound or freely) in {@code q},
     * {@code x} is said to be in the strict neighbourhood of a set {@code V} of bound variables if
     * <ol>
     *   <li>{@code x} is not an element of {@code V}, and</li>
     *   <li>{@code x} occurs in the subquery of {@code q} strictly induced by {@code V}.</li>
     * </ol>
     */
    public static ImmutableSet<Variable> neighbourhoodVariables(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> boundVariables
    ) {
        final var subquery = strictlyInduceSubqueryByBoundVariables(conjunctiveQuery, boundVariables);
        final var subqueryVariables = ImmutableSet
                .<Variable>builder()
                .addAll(Arrays.asList(subquery.getBoundVariables()))
                .addAll(Arrays.asList(subquery.getFreeVariables()))
                .build();
        final var boundVariableSet = ImmutableSet.copyOf(boundVariables);

        return SetExtensions.difference(subqueryVariables, boundVariableSet);
    }

    /**
     * Given a conjunctive query {@code q} and a set {@code v} of variables in {@code q},
     * checks if {@code v} is connected in {@code q}.
     * <p>
     * A set of variables {@code V} is said to be connected in {@code q} if
     * there is only one {@code q}-connected component of {@code V} in {@code q}.
     */
    public static boolean isConnected(
            final ConjunctiveQuery conjunctiveQuery,
            final Collection<? extends Variable> variables
    ) {
        if (variables.isEmpty()) {
            return true;
        }

        final var unionFindTree = new SimpleUnionFindTree<Variable>(variables);
        for (final var atom : conjunctiveQuery.getAtoms()) {
            final var variablesToUnion = SetExtensions.intersection(
                    ImmutableSet.copyOf(atom.getVariables()),
                    variables
            );
            unionFindTree.unionAll(variablesToUnion);
        }

        return unionFindTree.getEquivalenceClasses().size() == 1;
    }

    /**
     * Given a conjunctive query {@code q}, returns a stream of all {@code q}-connected
     * nonempty sets of {@code q}-bound variables.
     */
    public static Stream</* nonempty */ImmutableSet<Variable>> allConnectedBoundVariableSets(
            final ConjunctiveQuery conjunctiveQuery
    ) {
        final var boundVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());
        return SetExtensions.powerset(boundVariables)
                .filter(variableSet -> !variableSet.isEmpty())
                .filter(variableSet -> isConnected(conjunctiveQuery, variableSet));
    }
}
