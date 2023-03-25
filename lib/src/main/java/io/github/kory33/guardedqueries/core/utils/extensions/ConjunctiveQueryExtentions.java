package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Collection;

public class ConjunctiveQueryExtentions {
    private ConjunctiveQueryExtentions() {
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
    public static ConjunctiveQuery induceSubqueryByBoundVariables(
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
}
