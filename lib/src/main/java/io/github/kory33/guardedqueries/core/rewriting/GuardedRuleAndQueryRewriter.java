package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogQuery;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.StringSetExtensions;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Collection;
import java.util.stream.Stream;

public record GuardedRuleAndQueryRewriter(AbstractSaturation<? extends GTGD> saturation) {
    private record BoundVariableConnectedComponentRewriteResult(
            ImmutableCollection<? extends TGD> additionalRules,
            Predicate goalPredicate,
            ImmutableList<Variable> goalPredicateCanonicalVariables
    ) {
    }

    private BoundVariableConnectedComponentRewriteResult rewriteBoundVariableConnectedComponent(
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRules,
            final ConjunctiveQuery boundVariableConnectedSubquery,
            final String intentionalPredicatePrefix
    ) {
        final var queryFreeVariables = boundVariableConnectedSubquery.getFreeVariables();
        final var subqueryGoalPredicate = Predicate.create(
                intentionalPredicatePrefix + "_GOAL",
                queryFreeVariables.length
        );

        // TODO: Use SubqueryEntailmentComputation to compute the subquery entailment
        //       and then map all subquery-entailments to the corresponding subgoals.
        Collection<TGD> subgoalDerivationRules = null;

        // TODO: Generate all "glueing rules" to glue the subgoals into the goal predicate.
        //       For each vertex set V, we need to generate a rule of the form
        //         (subquery strongly induced by V) ∧ (subgoals for all connected components of (query\V))
        //           → subqueryGoalPredicate(V)
        Collection<TGD> subgoalGlueingRules = null;

        return new BoundVariableConnectedComponentRewriteResult(
                ImmutableSet.<TGD>builder().addAll(subgoalDerivationRules).addAll(subgoalGlueingRules).build(),
                subqueryGoalPredicate,
                ImmutableList.copyOf(queryFreeVariables)
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
                    bvccRewriteResults
                            .stream()
                            .map(bvccRewriteResult -> Atom.create(
                                    bvccRewriteResult.goalPredicate,
                                    bvccRewriteResult.goalPredicateCanonicalVariables.toArray(Term[]::new)
                            ))
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
