package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogQuery;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.utils.CachingFunction;
import io.github.kory33.guardedqueries.core.utils.extensions.*;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public record GuardedRuleAndQueryRewriter(AbstractSaturation<? extends GTGD> saturation) {
    private record BoundVariableConnectedComponentRewriteResult(
            ImmutableCollection<? extends TGD> additionalRules,
            Atom goalAtom
    ) {
    }

    private BoundVariableConnectedComponentRewriteResult rewriteBoundVariableConnectedComponent(
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRules,
            final /* bound-variable-connected */ ConjunctiveQuery boundVariableConnectedSubquery,
            final String intentionalPredicatePrefix
    ) {
        final var queryFreeVariables = boundVariableConnectedSubquery.getFreeVariables();
        final var subqueryGoalPredicate = Predicate.create(
                intentionalPredicatePrefix + "_GOAL",
                queryFreeVariables.length
        );
        final var subqueryGoalAtom = Atom.create(
                subqueryGoalPredicate,
                VariableSetExtensions
                        .sortBySymbol(Arrays.asList(queryFreeVariables))
                        .toArray(Term[]::new)
        );

        // A mapping from a set V of connected variables to the predicate that asserts that
        // the query has been existentially satisfied for the variables in V at the neighborhood of V.
        // The function may throw an exception on evaluation if the given set of variables contains free variables.
        final CachingFunction<ImmutableSet</* subquery-bound */ Variable>, Atom> subgoalAtoms;
        {
            final var predicateGeneratingCounter = new AtomicInteger(0);
            subgoalAtoms = new CachingFunction<>(variableSet -> {
                final var neighbourhood = ConjunctiveQueryExtensions.neighbourhoodVariables(
                        boundVariableConnectedSubquery,
                        variableSet
                );
                final var symbol = intentionalPredicatePrefix + "_SGL_" + predicateGeneratingCounter.getAndIncrement();
                final var subgoalPredicate = Predicate.create(symbol, neighbourhood.size());
                final var orderedNeighbourhood = VariableSetExtensions.sortBySymbol(neighbourhood);
                return Atom.create(subgoalPredicate, orderedNeighbourhood.toArray(Term[]::new));
            });
        }

        // TODO: Use SubqueryEntailmentComputation to compute the subquery entailment
        //       and then map all subquery-entailments to the corresponding subgoals.
        Collection<TGD> subgoalDerivationRules = null;

        final Collection<TGD> subgoalGlueingRules;
        {
            final java.util.function.Predicate<Predicate> isSubgoalPredicate = (Predicate predicate) ->
                    predicate.getName().startsWith(intentionalPredicatePrefix + "_SGL_");

            subgoalGlueingRules = SetExtensions
                    .powerset(Arrays.asList(boundVariableConnectedSubquery.getBoundVariables()))
                    .<TGD>map(existentialWitnessCandidate -> {
                        // A single existentialWitnessCandidate is a set of variables that the rule
                        // (which we are about to produce) expects to be existentially satisfied.
                        //
                        // We call the complement of existentialWitnessCandidate as baseWitnessVariables,
                        // since we expect (within the rule we are about to produce) those variables to be witnessed
                        // by values in the base instance.
                        //
                        // The rule that we need to produce, therefore, will be of the form
                        //   (subquery strongly induced by baseWitnessVariables,
                        //    except there is no existential quantification)
                        // ∧ (for each connected component V of existentialWitnessCandidate,
                        //    a subgoal atom corresponding to V)
                        //  → subqueryGoalAtom
                        //
                        // In the following code, we call the first conjunct of the rule "baseWitnessJoinConditions",
                        // the second conjunct "neighbourhoodsSubgoals".

                        final var baseWitnessVariables = SetExtensions.difference(
                                existentialWitnessCandidate,
                                SetExtensions.union(
                                        Arrays.asList(boundVariableConnectedSubquery.getBoundVariables()),
                                        Arrays.asList(boundVariableConnectedSubquery.getFreeVariables())
                                )
                        );

                        final var baseWitnessJoinConditions = ConjunctiveQueryExtensions
                                .strictlyInduceSubqueryByVariables(
                                        boundVariableConnectedSubquery,
                                        baseWitnessVariables
                                ).getAtoms();

                        final var neighbourhoodsSubgoals = ConjunctiveQueryExtensions
                                .connectedComponents(
                                        boundVariableConnectedSubquery,
                                        existentialWitnessCandidate
                                )
                                .map(subgoalAtoms)
                                .toArray(Atom[]::new);

                        return new DatalogRule(
                                Stream.concat(
                                        Arrays.stream(baseWitnessJoinConditions),
                                        Arrays.stream(neighbourhoodsSubgoals)
                                ).toArray(Atom[]::new),
                                new Atom[]{subqueryGoalAtom}
                        );
                    })
                    .toList();
        }

        return new BoundVariableConnectedComponentRewriteResult(
                SetExtensions.union(subgoalDerivationRules, subgoalGlueingRules),
                subqueryGoalAtom
        );
    }

    /**
     * Compute the Datalog rewriting of a finite set of GTGD rules and a conjunctive query.
     */
    public DatalogQuery rewrite(final Collection<? extends GTGD> rules, final ConjunctiveQuery query) {
        final var initialSignature = FunctionFreeSignature.encompassingRuleQuery(rules, query);
        final var intentionalPredicatePrefix = StringSetExtensions.freshPrefix(
                initialSignature.predicateNames(),
                // stands for Intentional Predicates
                "IP"
        );

        final var normalizedRules = NormalGTGD.normalize(
                rules,
                // stands for Normalization-Intermediate predicates
                intentionalPredicatePrefix + "_NI"
        );
        final var saturatedRuleSet = new SaturatedRuleSet<>(saturation, normalizedRules);
        final var cqConnectedComponents = new CQBoundVariableConnectedComponents(query);

        final var bvccRewriteResults = StreamExtensions
                .zipWithIndex(cqConnectedComponents.maximallyConnectedSubqueries.stream())
                .map(pair -> {
                    final var maximallyConnectedSubquery = pair.getLeft();

                    // prepare a prefix for intentional predicates that may be introduced to rewrite a
                    // maximally connected subquery. "SQ" stands for "subquery".
                    final var subqueryIntentionalPredicatePrefix =
                            intentionalPredicatePrefix + "_SQ" + pair.getRight();

                    return this.rewriteBoundVariableConnectedComponent(
                            saturatedRuleSet,
                            maximallyConnectedSubquery,
                            subqueryIntentionalPredicatePrefix
                    );
                })
                .toList();

        final Predicate goalPredicate = Predicate.create(
                intentionalPredicatePrefix + "_GOAL",
                query.getFreeVariables().length
        );

        // the rule to "join" all subquery results
        final TGD finalJoinRule;
        {
            // we have to join all of
            //  - bound-variable-free atoms
            //  - goal predicates of each maximally connected subquery
            final var bodyAtoms = Stream.concat(
                    cqConnectedComponents.boundVariableFreeAtoms.stream(),
                    bvccRewriteResults.stream().map(BoundVariableConnectedComponentRewriteResult::goalAtom)
            ).toArray(Atom[]::new);

            // ... to derive the final goal predicate
            final var headAtom = Atom.create(goalPredicate, query.getFreeVariables());

            finalJoinRule = TGD.create(bodyAtoms, new Atom[]{headAtom});
        }

        final var allRules = ImmutableSet
                .<TGD>builder()
                .addAll(saturatedRuleSet.saturatedRules)
                .addAll(bvccRewriteResults
                        .stream()
                        .map(BoundVariableConnectedComponentRewriteResult::additionalRules)
                        .flatMap(Collection::stream)
                        .toList()
                )
                .add(finalJoinRule)
                .build();

        return new DatalogQuery(DatalogProgram.tryFromDependencies(allRules), goalPredicate);
    }
}
