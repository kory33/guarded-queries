package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogQuery;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalName;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentComputation;
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree;
import io.github.kory33.guardedqueries.core.utils.extensions.*;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

        final var subgoalAtoms = new SubgoalAtomGenerator(
                boundVariableConnectedSubquery,
                intentionalPredicatePrefix + "_SGL_"
        );

        final Collection<NormalGTGD.FullGTGD> subgoalDerivationRules =
                new SubqueryEntailmentComputation(saturatedRules, boundVariableConnectedSubquery)
                        .run()
                        .map(subqueryEntailment -> {
                            final var ruleConstantWitnessGuess = subqueryEntailment.ruleConstantWitnessGuess();
                            final var coexistentialVariables = subqueryEntailment.coexistentialVariables();
                            final var localWitnessGuess = subqueryEntailment.localWitnessGuess();
                            final var localInstance = subqueryEntailment.localInstance();

                            // For example, suppose that the subquery entailment instance
                            //     <{ x ↦ a }, {z, w}, { y ↦ 2 }, { { R(2,1,3), U(1), P(2,3) } }>
                            // in fact entails the subquery goal SGL_{z,w}(x, y) according to the
                            // SubqueryEntailmentComputation::run() we have just invoked.
                            // We then add a rule of the form
                            //     R(y,_f1,_f3) ∧ U(_f1) ∧ P(y,_f3) → SGL_{z,w}(a,y)
                            // where _f1 and _f3 are fresh variables and SGL_{z,w}(x,y) is the subgoal atom
                            // provided by subgoalAtoms object.
                            //
                            // In general, for a subquery entailment instance <C, V, L, I>,
                            // we need to produce a rule of the form
                            //     (I with each local name pulled back and unified by L, except that
                            //      local names outside the range of L are consistently
                            //      replaced by fresh variables)
                            //     → (the subgoal atom corresponding to V, except that
                            //        the same unification (by L) done to the premise is performed
                            //        and the variables in C are replaced by their preimages
                            //        (hence some constant) in C)
                            //
                            // This process produces a full GTGD for each subquery entailment,
                            // and this is what we should be doing in this .map() call.

                            // Mapping of local names to their preimages in the neighbourhood mapping.
                            // Contains all active local names in the key set,
                            // and the range of the mapping is a partition of variables mapped by localWitnessGuess.
                            final ImmutableMap<LocalName, /* possibly empty, disjoint */ImmutableSet<Variable>> neighbourhoodPreimages =
                                    MapExtensions.preimages(localWitnessGuess, localInstance.getActiveTerms());

                            // unification of variables mapped by localWitnessGuess to fresh variables
                            final ImmutableMap</* domain of localWitnessGuess */Variable, /* fresh */Variable> unification;
                            {
                                final ImmutableSet<Variable> localWitnessGuessDomain =
                                        ImmutableSet.copyOf(localWitnessGuess.keySet());

                                final ImmutableSet<ImmutableSet<Variable>> equivalenceClasses;
                                {
                                    final var unionFind = new SimpleUnionFindTree<>(localWitnessGuessDomain);
                                    for (final var unionedVariables : neighbourhoodPreimages.values()) {
                                        unionFind.unionAll(unionedVariables);
                                    }
                                    equivalenceClasses = unionFind.getEquivalenceClasses();
                                }

                                final ImmutableMap<ImmutableSet<Variable>, Variable> unifiedVariableMap =
                                        ImmutableMapExtensions.consumeAndCopy(
                                                equivalenceClasses
                                                        .stream()
                                                        .map(c -> Map.entry(c, Variable.getFreshVariable()))
                                                        .iterator()
                                        );

                                final var unificationMapBuilder = ImmutableMap.<Variable, Variable>builder();
                                for (final var equivalenceClass : equivalenceClasses) {
                                    final var unifiedVariable = unifiedVariableMap.get(equivalenceClass);
                                    for (final var variable : equivalenceClass) {
                                        unificationMapBuilder.put(variable, unifiedVariable);
                                    }
                                }

                                unification = unificationMapBuilder.build();
                            }


                            // Mapping of local names to terms.
                            // Contains all active local names in the key set.
                            final ImmutableMap<LocalName, Term> nameToTermMap =
                                    ImmutableMapExtensions.consumeAndCopy(
                                            StreamExtensions
                                                    .associate(localInstance.getActiveTerms().stream(), localName -> {
                                                        final var preimage = neighbourhoodPreimages.get(localName);
                                                        if (preimage.isEmpty()) {
                                                            // if this local name is not in the range of localWitnessGuess,
                                                            // we assign a fresh variable to represent the genericity
                                                            // of the local name
                                                            return Variable.getFreshVariable();
                                                        } else {
                                                            // otherwise unify
                                                            final var unifiedVariable = unification.get(preimage.iterator().next());
                                                            assert unifiedVariable != null;
                                                            return unifiedVariable;
                                                        }
                                                    })
                                                    .iterator()
                                    );

                            final var mappedInstance = subqueryEntailment.localInstance().map(nameToTermMap::get);

                            final var subgoalAtom = subgoalAtoms.apply(coexistentialVariables);
                            final var mappedSubgoalAtom = Atom.create(
                                    subgoalAtom.getPredicate(),
                                    Arrays.stream(subgoalAtom.getTerms())
                                            .map(term -> (Variable) term /* safe, since only variables are applied to subgoal atoms */)
                                            .map(variable -> {
                                                if (unification.containsKey(variable)) {
                                                    return unification.get(variable);
                                                } else if (ruleConstantWitnessGuess.containsKey(variable)) {
                                                    return ruleConstantWitnessGuess.get(variable);
                                                } else {
                                                    // The contract of SubqueryEntailmentComputation::run() is that
                                                    // the returned subquery entailment instance is a valid one,
                                                    // which means that the neighbourhood of coexistential variables
                                                    // must be covered by domains of localWitnessGuess and ruleConstantWitnessGuess.
                                                    //
                                                    // If this is not satisfied, something has gone wrong, and we must raise an error.
                                                    throw new AssertionError(
                                                            "Variable " + variable + " is not mapped by either unification or ruleConstantWitnessGuess"
                                                    );
                                                }
                                            })
                                            .toArray(Term[]::new)
                            );

                            // The contract of SubqueryEntailmentComputation::run() is that
                            // the instance is guarded by some atom. Since we have unified the instance
                            // together with the subgoal atom,
                            // - mappedSubgoalAtom contains no existential variables, and
                            // - there must be a guard in the mapped instance
                            return new NormalGTGD.FullGTGD(
                                    FormalInstance.asAtoms(mappedInstance),
                                    List.of(mappedSubgoalAtom)
                            );
                        })
                        .toList();

        final Collection<TGD> subgoalGlueingRules = SetExtensions
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
                            .map(subgoalAtoms::apply)
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
