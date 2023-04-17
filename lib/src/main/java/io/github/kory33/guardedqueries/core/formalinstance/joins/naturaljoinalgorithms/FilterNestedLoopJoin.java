package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult;
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm;
import io.github.kory33.guardedqueries.core.formalinstance.joins.SingleAtomMatching;
import io.github.kory33.guardedqueries.core.utils.extensions.ImmutableMapExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A join algorithm that first filters tuples matching query atoms and then joins them in a nested loop fashion.
 */
public class FilterNestedLoopJoin<TA> implements NaturalJoinAlgorithm<TA, FormalInstance<TA>> {
    /**
     * Recursively nest matching loops to extend the given partial homomorphism,
     * in the order specified by {@code remainingAtomsToJoin}.
     * <p>
     * Each result of the successful join is passed to {@code visitEachCompleteHomomorphism}.
     * <p>
     * As a successful join uniquely determines the sequence of tuples in join results
     * corresponding to the sequence of query atoms, as long as each {@code JoinResult} in
     * {@code atomToMatches} does not contain duplicate tuples, no duplicate tuples will be
     * passed to {@code visitEachCompleteHomomorphism}.
     */
    private static <TA> void visitAllJoinResults(
            ImmutableList<Atom> remainingAtomsToJoin,
            ImmutableMap<Atom, JoinResult<TA>> atomToMatches,
            ImmutableList<Variable> resultVariableOrdering,
            ImmutableList<Optional<TA>> partialHomomorphism,
            Consumer<ImmutableList<TA>> visitEachCompleteHomomorphism
    ) {
        if (remainingAtomsToJoin.isEmpty()) {
            // If there are no more atoms to join, the given partial homomorphism
            // should be complete. So unwrap Optionals and visit
            //noinspection OptionalGetWithoutIsPresent
            final var unwrappedHomomorphism = ImmutableList.copyOf(
                    partialHomomorphism.stream().map(Optional::get).iterator()
            );
            visitEachCompleteHomomorphism.accept(unwrappedHomomorphism);
            return;
        }

        final var nextAtomMatchResult = atomToMatches.get(remainingAtomsToJoin.get(0));
        final var matchVariableOrdering = nextAtomMatchResult.variableOrdering();
        final var nextAtomMatches = nextAtomMatchResult.allHomomorphisms();

        iterateThroughMatches:
        for (final var match : nextAtomMatches) {
            final var homomorphismExtension = new ArrayList<>(partialHomomorphism);

            for (int matchVariableIndex = 0; matchVariableIndex < matchVariableOrdering.size(); matchVariableIndex++) {
                final var nextVariableToCheck = matchVariableOrdering.get(matchVariableIndex);
                final var extensionCandidate = match.get(matchVariableIndex);

                final var indexOfVariableInResultOrdering = resultVariableOrdering.indexOf(nextVariableToCheck);
                final var mappingSoFar = homomorphismExtension.get(indexOfVariableInResultOrdering);

                if (mappingSoFar.isEmpty()) {
                    // we are free to extend the homomorphism to the variable
                    homomorphismExtension.set(
                            indexOfVariableInResultOrdering,
                            Optional.of(extensionCandidate)
                    );
                } else if (!mappingSoFar.get().equals(extensionCandidate)) {
                    // the match cannot extend the partialHomomorphism, so skip to the next match
                    continue iterateThroughMatches;
                }
            }

            // at this point the match must have been extended to cover all variables in the atom,
            // so proceed
            visitAllJoinResults(
                    remainingAtomsToJoin.subList(0, remainingAtomsToJoin.size()),
                    atomToMatches,
                    resultVariableOrdering,
                    ImmutableList.copyOf(partialHomomorphism),
                    visitEachCompleteHomomorphism
            );
        }
    }

    @Override
    public JoinResult<TA> join(ConjunctiveQuery query, FormalInstance<TA> formalInstance) {
        final var queryAtoms = ImmutableSet.copyOf(query.getAtoms());

        // we throw IllegalArgumentException if the query contains existential atoms
        if (query.getBoundVariables().length != 0) {
            throw new IllegalArgumentException("NestedLoopJoin does not support existential queries");
        }

        final var queryPredicates = Arrays.stream(query.getAtoms())
                .map(Atom::getPredicate)
                .collect(Collectors.toSet());

        final ImmutableMap<Predicate, FormalInstance<TA>> relevantRelationsToInstancesMap;
        {
            final var relevantFactsStream = formalInstance.facts.stream()
                    .filter(fact -> queryPredicates.contains(fact.predicate()));

            final var groupedFacts =
                    relevantFactsStream.collect(Collectors.groupingBy(FormalFact::predicate));

            relevantRelationsToInstancesMap = MapExtensions.composeWithFunction(groupedFacts, FormalInstance::new);
        }

        final ImmutableMap<Atom, JoinResult<TA>> queryAtomsToMatches = ImmutableMapExtensions.consumeAndCopy(
                StreamExtensions.associate(
                        queryAtoms.stream(),
                        atom -> SingleAtomMatching.allMatches(
                                atom,
                                relevantRelationsToInstancesMap.getOrDefault(atom.getPredicate(), FormalInstance.empty())
                        )
                ).iterator()
        );

        // we start joining from the outer
        final var queryAtomsOrderedByMatchSizes = ImmutableList.copyOf(
                queryAtomsToMatches.keySet().stream()
                        .sorted(Comparator.comparing(atom -> queryAtomsToMatches.get(atom).allHomomorphisms().size()))
                        .iterator()
        );

        final var queryVariableOrdering = ImmutableList.copyOf(ImmutableSet.copyOf(
                Arrays.stream(query.getAtoms()).flatMap(atom -> Arrays.stream(atom.getVariables())).iterator()
        ));

        final var emptyHomomorphism = ImmutableList.copyOf(
                queryVariableOrdering.stream().map(v -> Optional.<TA>empty()).iterator()
        );

        final var resultBuilder = ImmutableList.<ImmutableList<TA>>builder();
        visitAllJoinResults(
                queryAtomsOrderedByMatchSizes, queryAtomsToMatches, queryVariableOrdering, emptyHomomorphism,
                resultBuilder::add
        );
        return new JoinResult<>(queryVariableOrdering, resultBuilder.build());
    }
}
