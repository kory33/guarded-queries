package io.github.kory33.guardedqueries.core.formalinstance.joins;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.ArrayList;
import java.util.Optional;

public class SingleAtomMatching {
    private SingleAtomMatching() {
    }

    private static <TA> Optional<ImmutableList<TA>> tryMatch(
            final Atom atomicQuery,
            final ImmutableList<Variable> orderedQueryVariables,
            final ImmutableList<TA> appliedTerms
    ) {
        final var homomorphism = new ArrayList<Optional<TA>>(orderedQueryVariables.size());
        for (int i = 0; i < orderedQueryVariables.size(); i++) {
            homomorphism.add(Optional.empty());
        }

        for (int appliedTermIndex = 0; appliedTermIndex < appliedTerms.size(); appliedTermIndex++) {
            final var termToMatch = atomicQuery.getTerms()[appliedTermIndex];
            final var appliedTerm = appliedTerms.get(appliedTermIndex);

            if (termToMatch instanceof Constant) {
                // if the term is a constant, we just check if that constant has been applied
                if (!termToMatch.equals(appliedTerm)) {
                    // and fail if not
                    return Optional.empty();
                }
            } else if (termToMatch instanceof Variable) {
                final var variableIndex = orderedQueryVariables.indexOf(termToMatch);
                final var alreadyAssignedConstant = homomorphism.get(variableIndex);

                if (alreadyAssignedConstant.isPresent()) {
                    // if the variable has already been assigned a constant, we check if the constant is the same
                    if (!alreadyAssignedConstant.get().equals(appliedTerm)) {
                        // and fail if not
                        return Optional.empty();
                    }
                } else {
                    // if the variable has not already been assigned a constant, we assign it
                    homomorphism.set(variableIndex, Optional.of(appliedTerm));
                }
            }
        }

        // if we have reached this point, we have successfully matched all variables in the query
        // to constants applied to the fact, so return the homomorphism

        //noinspection OptionalGetWithoutIsPresent
        final var unwrappedHomomorphism = ImmutableList.copyOf(
                homomorphism.stream()
                        .map(Optional::get)
                        .iterator()
        );

        return Optional.of(unwrappedHomomorphism);
    }

    /**
     * Finds all answers to the given atomic query in the given instance.
     * <p>
     * The returned join result is well-formed.
     *
     * @throws IllegalArgumentException if the given query contains a term that is neither a variable nor a constant
     */
    public static <TA> JoinResult<TA> allMatches(
            final Atom atomicQuery,
            final FormalInstance<TA> instance
    ) {
        final var orderedQueryVariables = ImmutableList.copyOf(ImmutableSet.copyOf(
                atomicQuery.getVariables()
        ));

        final var queryPredicate = atomicQuery.getPredicate();

        final var homomorphisms = ImmutableList.<ImmutableList<TA>>builder();
        for (final var fact : instance.facts) {
            if (!fact.predicate().equals(queryPredicate)) {
                continue;
            }

            // compute a homomorphism and add to the builder, or continue to the next fact if we cannot do so
            tryMatch(atomicQuery, orderedQueryVariables, fact.appliedTerms()).ifPresent(homomorphisms::add);
        }

        return new JoinResult<>(orderedQueryVariables, homomorphisms.build());
    }
}
