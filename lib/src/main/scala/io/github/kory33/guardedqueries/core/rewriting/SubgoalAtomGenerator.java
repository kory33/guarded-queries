package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A mapping that sends a set V of connected variables to an atom that asserts
 * that the query has been existentially satisfied for the variables in V
 * with a particular instantiation of the neighborhood of V.
 */
public class SubgoalAtomGenerator {
    private final CachingFunction</* query-connected */ImmutableSet</* query-bound */ Variable>, Atom> subgoalAtoms;

    public SubgoalAtomGenerator(
            final ConjunctiveQuery boundVariableConnectedQuery,
            final String intentionalPredicatePrefix
    ) {
        {
            final var connectedComponents = new CQBoundVariableConnectedComponents(boundVariableConnectedQuery);
            if (connectedComponents.maximallyConnectedSubqueries.size() > 1) {
                throw new IllegalArgumentException(
                        "The given query (" + boundVariableConnectedQuery + ") is not bound-variable-connected."
                );
            }
            if (!connectedComponents.boundVariableFreeAtoms.isEmpty()) {
                throw new IllegalArgumentException(
                        "The given query (" + boundVariableConnectedQuery + ") contains bound-variable-free atoms."
                );
            }
        }

        final var queryBoundVariables = ImmutableSet.copyOf(boundVariableConnectedQuery.getBoundVariables());
        final var predicateGeneratingCounter = new AtomicInteger(0);

        //noinspection Convert2Diamond (IDEA erronously gives an error if we remove explicit arguments)
        this.subgoalAtoms = new CachingFunction<ImmutableSet<Variable>, Atom>(variableSet -> {
            // by the contract, we can (and should) reject variable sets that
            //  - are not connected, or
            //  - contain non-bound variables
            if (!ConjunctiveQueryExtensions.isConnected(boundVariableConnectedQuery, variableSet)) {
                throw new IllegalArgumentException(
                        "The given set of variables (" + variableSet + ") is not connected in the given query ("
                                + boundVariableConnectedQuery + ")."
                );
            }
            if (!queryBoundVariables.containsAll(variableSet)) {
                throw new IllegalArgumentException(
                        "The given set of variables (" + variableSet + ") contains non-bound variables in the given query ("
                                + boundVariableConnectedQuery + ")."
                );
            }

            final var neighbourhood = ConjunctiveQueryExtensions.neighbourhoodVariables(
                    boundVariableConnectedQuery,
                    variableSet
            );
            final var symbol = intentionalPredicatePrefix + "_" + predicateGeneratingCounter.getAndIncrement();
            final var subgoalPredicate = Predicate.create(symbol, neighbourhood.size());
            final var orderedNeighbourhood = VariableSetExtensions.sortBySymbol(neighbourhood);
            return Atom.create(subgoalPredicate, orderedNeighbourhood.toArray(Term[]::new));
        });
    }

    /**
     * Computes an atom corresponding to the given connected set of variables in a query,
     * while storing any generated atom to the cache for future use.
     * <p>
     * On generation of an atom, the predicate symbol of the atom is generated
     * by concatenating {@code intentionalPredicatePrefix} with an internal integer counter.
     *
     * @throws IllegalArgumentException if the given set of variables is disconnected or contains free variables.
     */
    public Atom apply(final Collection<? extends Variable> variableSet) {
        return this.subgoalAtoms.apply(ImmutableSet.copyOf(variableSet));
    }
}
