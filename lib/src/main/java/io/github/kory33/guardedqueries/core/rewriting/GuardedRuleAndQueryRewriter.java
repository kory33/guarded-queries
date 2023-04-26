package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.LocalVariableContext;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentComputation;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance;
import io.github.kory33.guardedqueries.core.utils.extensions.*;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

public record GuardedRuleAndQueryRewriter(
        AbstractSaturation<? extends GTGD> saturation,
        SubqueryEntailmentComputation subqueryEntailmentComputation
) {
    private record BoundVariableConnectedComponentRewriteResult(
            Atom goalAtom,
            ImmutableCollection<? extends DatalogRule> goalDerivationRules
    ) {
    }

    /**
     * Transforms a subquery entailment into a rule to derive a subgoal.
     * <p>
     * The instance {@code subqueryEntailment} must be a subquery entailment
     * associated to some rule-set (which we will not make use of in this method) and
     * the query {@code subgoalAtoms.query()}.
     * <p>
     * For example, suppose that the subquery entailment instance
     * <pre><{ x ↦ a }, {z, w}, { y ↦ 2 }, { { R(2,1,3), U(1), P(2,c) } }></pre>
     * where c is a constant from the rule-set, entails the subquery of {@code subgoalAtoms.query()}
     * relevant to {z, w}. Then we must add a rule of the form
     * <pre>R(y,_f1,_f3) ∧ U(_f1) ∧ P(y,c) → SGL_{z,w}(a,y)</pre>
     * where {@code _f1} and {@code _f3} are fresh variables and {@code SGL_{z,w}(x,y)} is the subgoal atom
     * provided by subgoalAtoms object.
     * <p>
     * In general, for each subquery entailment instance {@code <C, V, L, I>},
     * we need to produce a rule of the form
     * <pre>
     *  (I with each local name pulled back and unified by L, except that
     *   local names outside the range of L are consistently
     *   replaced by fresh variables)
     *  → (the subgoal atom corresponding to V, except that
     *     the same unification (by L) done to the premise is performed
     *     and the variables in C are replaced by their preimages
     *     (hence some constant) in C)
     * </pre>
     */
    private DatalogRule subqueryEntailmentRecordToSubgoalRule(
            final SubqueryEntailmentInstance subqueryEntailment,
            final SubgoalAtomGenerator subgoalAtoms
    ) {
        final var ruleConstantWitnessGuess = subqueryEntailment.ruleConstantWitnessGuess();
        final var coexistentialVariables = subqueryEntailment.coexistentialVariables();
        final var localWitnessGuess = subqueryEntailment.localWitnessGuess();
        final var localInstance = subqueryEntailment.localInstance();
        final var queryConstantEmbeddingInverse = subqueryEntailment.queryConstantEmbedding().inverse();

        // We prepare a variable context that is closed within the rule
        // we are about to generate. This is essential to reduce the memory usage
        // of generated rule set, because this way we are more likely to
        // generate identical atoms which can be cached.
        final var ruleLocalVariableContext = new LocalVariableContext("x_");

        final var activeLocalNames = localInstance.getActiveTerms().stream().flatMap(t -> {
            if (t instanceof LocalInstanceTerm.LocalName) {
                return Stream.of((LocalInstanceTerm.LocalName) t);
            } else {
                return Stream.empty();
            }
        }).toList();

        // Mapping of local names to their preimages in the neighbourhood mapping.
        // Contains all active local names in the key set,
        // and the range of the mapping is a partition of domain of localWitnessGuess.
        final ImmutableMap<LocalInstanceTerm.LocalName, /* possibly empty, disjoint */ImmutableSet<Variable>> neighbourhoodPreimages =
                MapExtensions.preimages(localWitnessGuess, activeLocalNames);

        // unification of variables mapped by localWitnessGuess to fresh variables
        final ImmutableMap</* domain of localWitnessGuess */Variable, /* fresh */Variable> unification;
        {
            final var unificationMapBuilder = ImmutableMap.<Variable, Variable>builder();
            for (final var equivalenceClass : neighbourhoodPreimages.values()) {
                final var unifiedVariable = ruleLocalVariableContext.getFreshVariable();
                for (final var variable : equivalenceClass) {
                    unificationMapBuilder.put(variable, unifiedVariable);
                }
            }

            unification = unificationMapBuilder.build();
        }


        // Mapping of local names to variables (or constants for local names bound to query-constant).
        // Contains all active local names in the key set.
        final ImmutableMap<LocalInstanceTerm.LocalName, Term> nameToTermMap =
                ImmutableMapExtensions.consumeAndCopy(
                        StreamExtensions.associate(activeLocalNames.stream(), localName -> {
                            final var preimage = neighbourhoodPreimages.get(localName);
                            if (preimage.isEmpty()) {
                                if (queryConstantEmbeddingInverse.containsKey(localName)) {
                                    // if this local name is bound to a query constant,
                                    // we assign the query constant to the local name
                                    return queryConstantEmbeddingInverse.get(localName);
                                } else {
                                    // the local name is bound neither to a query constant nor
                                    // query-bound variable, so we assign a fresh variable to it
                                    return ruleLocalVariableContext.getFreshVariable();
                                }
                            } else {
                                // the contract of SubqueryEntailmentComputation guarantees that
                                // local names bound to bound variables should not be bound
                                // to a query constant
                                assert !queryConstantEmbeddingInverse.containsKey(localName);

                                // otherwise unify to the variable corresponding to the preimage
                                // e.g. if {x, y} is the preimage of localName and _xy is the variable
                                // corresponding to {x, y}, we turn localName into _xy
                                final var unifiedVariable = unification.get(preimage.iterator().next());
                                assert unifiedVariable != null;
                                return unifiedVariable;
                            }
                        }).iterator()
                );

        final var mappedInstance = localInstance.map(t -> t.mapLocalNamesToTerm(nameToTermMap::get));

        final Atom mappedSubgoalAtom;
        {
            final var subgoalAtom = subgoalAtoms.apply(coexistentialVariables);

            final var orderedNeighbourhoodVariables = Arrays
                    .stream(subgoalAtom.getTerms())
                    .map(term -> (Variable) term /* safe, since only variables are applied to subgoal atoms */);

            final java.util.function.Function<Variable, Term> neighbourhoodVariableToTerm = variable -> {
                if (unification.containsKey(variable)) {
                    return unification.get(variable);
                } else if (ruleConstantWitnessGuess.containsKey(variable)) {
                    return ruleConstantWitnessGuess.get(variable);
                } else {
                    // The contract ensures that the given subquery entailment instance is a valid instance
                    // with respect to the whole query (subgoalAtoms.query()), which means that
                    // the neighbourhood of coexistential variables must be covered
                    // by the union of domains of localWitnessGuess and ruleConstantWitnessGuess.
                    throw new AssertionError(
                            "Variable " + variable + " is not mapped by either unification or ruleConstantWitnessGuess"
                    );
                }
            };

            final var replacedTerms = orderedNeighbourhoodVariables
                    .map(neighbourhoodVariableToTerm)
                    .toArray(Term[]::new);

            mappedSubgoalAtom = Atom.create(subgoalAtom.getPredicate(), replacedTerms);
        }

        // subgoalAtom has variables in the neighbourhood of coexistentialVariables as its parameters.
        // On the other hand, every variable in the neighbourhood of coexistentialVariables is mapped
        // either
        //  1. by ruleConstantWitnessGuess to a constant appearing in the rule, or
        //  2. by localWitnessGuess to a local name active in localInstance, which is then unified by unification,
        // and these mappings are applied uniformly across localInstance and subgoalAtom.
        //
        // Therefore, every variable appearing in mappedSubgoalAtom is a variable produced by unification map,
        // which must also occur in some atom of mappedInstance (as the local name
        // to which the unified variables were sent was active in localInstance).
        // Hence, the rule (mappedInstance → mappedSubgoalAtom) is a Datalog rule.
        return new DatalogRule(
                FormalInstance.asAtoms(mappedInstance).toArray(Atom[]::new),
                new Atom[]{mappedSubgoalAtom}
        );
    }

    /**
     * Rewrite a bound-variable-connected query {@code boundVariableConnectedQuery} into a pair of
     * <ol>
     *   <li>a fresh goal atom for the query.</li>
     *   <li>a set of additional rules that, when run on a saturated base data, produces all answers to
     *       {@code boundVariableConnectedQuery}</li>
     * </ol>
     */
    private BoundVariableConnectedComponentRewriteResult rewriteBoundVariableConnectedComponent(
            final FunctionFreeSignature extensionalSignature,
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRules,
            final /* bound-variable-connected */ ConjunctiveQuery boundVariableConnectedQuery,
            final String intentionalPredicatePrefix
    ) {
        final Atom queryGoalAtom;
        {
            final var queryFreeVariables = boundVariableConnectedQuery.getFreeVariables();
            final var goalPredicate = Predicate.create(
                    intentionalPredicatePrefix + "_GOAL",
                    ImmutableSet.copyOf(queryFreeVariables).size()
            );
            final var sortedFreeVariables = VariableSetExtensions
                    .sortBySymbol(Arrays.asList(queryFreeVariables))
                    .toArray(Variable[]::new);
            queryGoalAtom = Atom.create(goalPredicate, sortedFreeVariables);
        }

        final var subgoalAtoms = new SubgoalAtomGenerator(
                boundVariableConnectedQuery,
                intentionalPredicatePrefix + "_SGL"
        );

        final Collection<DatalogRule> subgoalDerivationRules =
                subqueryEntailmentComputation
                        .apply(extensionalSignature, saturatedRules, boundVariableConnectedQuery)
                        .map(subqueryEntailment -> subqueryEntailmentRecordToSubgoalRule(subqueryEntailment, subgoalAtoms))
                        .toList();

        final Collection<DatalogRule> subgoalGlueingRules = SetLikeExtensions
                .powerset(Arrays.asList(boundVariableConnectedQuery.getBoundVariables()))
                .map(existentialWitnessCandidate -> {
                    // A single existentialWitnessCandidate is a set of variables that the rule
                    // (which we are about to produce) expects to be existentially satisfied.
                    //
                    // We call the complement of existentialWitnessCandidate as baseWitnessVariables,
                    // since we expect (within the rule we are about to produce) those variables to be witnessed
                    // by values in the base instance.
                    //
                    // The rule that we need to produce, therefore, will be of the form
                    //   (subquery of boundVariableConnectedQuery strongly induced by baseWitnessVariables,
                    //    except we turn all existential quantifications to universal quantifications)
                    // ∧ (for each connected component V of existentialWitnessCandidate,
                    //    a subgoal atom corresponding to V)
                    //  → queryGoalAtom
                    //
                    // In the following code, we call the first conjunct of the rule "baseWitnessJoinConditions",
                    // the second conjunct "neighbourhoodsSubgoals".

                    final var baseWitnessVariables = SetLikeExtensions.difference(
                            ConjunctiveQueryExtensions.variablesIn(boundVariableConnectedQuery),
                            existentialWitnessCandidate
                    );

                    final var baseWitnessJoinConditions = ConjunctiveQueryExtensions
                            .strictlyInduceSubqueryByVariables(
                                    boundVariableConnectedQuery,
                                    baseWitnessVariables
                            )
                            .map(ConjunctiveQuery::getAtoms)
                            .orElseGet(() -> new Atom[0]);

                    final var neighbourhoodsSubgoals = ConjunctiveQueryExtensions
                            .connectedComponents(
                                    boundVariableConnectedQuery,
                                    existentialWitnessCandidate
                            )
                            .map(subgoalAtoms::apply)
                            .toArray(Atom[]::new);

                    return new DatalogRule(
                            Stream.concat(
                                    Arrays.stream(baseWitnessJoinConditions),
                                    Arrays.stream(neighbourhoodsSubgoals)
                            ).toArray(Atom[]::new),
                            new Atom[]{queryGoalAtom}
                    );
                })
                .toList();

        return new BoundVariableConnectedComponentRewriteResult(
                queryGoalAtom,
                SetLikeExtensions.union(subgoalDerivationRules, subgoalGlueingRules)
        );
    }

    /**
     * Compute a Datalog rewriting of a finite set of GTGD rules and a conjunctive query.
     */
    public DatalogRewriteResult rewrite(final Collection<? extends GTGD> rules, final ConjunctiveQuery query) {
        // Set of predicates that may appear in the input database.
        // Any predicate not in this signature can be considered as intentional predicates
        // and may be ignored in certain cases, such as when generating "test" instances.
        final var extensionalSignature = FunctionFreeSignature.encompassingRuleQuery(rules, query);

        final var intentionalPredicatePrefix = StringSetExtensions.freshPrefix(
                extensionalSignature.predicateNames(),
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
                            extensionalSignature,
                            saturatedRuleSet,
                            maximallyConnectedSubquery,
                            subqueryIntentionalPredicatePrefix
                    );
                })
                .toList();

        final var deduplicatedQueryVariables =
                ImmutableList.copyOf(ImmutableSet.copyOf(query.getFreeVariables()));

        final var goalPredicate = Predicate.create(
                intentionalPredicatePrefix + "_GOAL",
                deduplicatedQueryVariables.size()
        );
        final var goalAtom = Atom.create(goalPredicate, deduplicatedQueryVariables.toArray(Variable[]::new));

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
            finalJoinRule = TGD.create(bodyAtoms, new Atom[]{goalAtom});
        }

        final var allDerivationRules = ImmutableSet
                .<TGD>builder()
                .addAll(bvccRewriteResults
                        .stream()
                        .map(BoundVariableConnectedComponentRewriteResult::goalDerivationRules)
                        .flatMap(Collection::stream)
                        .toList()
                )
                .add(finalJoinRule)
                .build();

        return new DatalogRewriteResult(
                DatalogProgram.tryFromDependencies(saturatedRuleSet.saturatedRules),
                DatalogProgram.tryFromDependencies(allDerivationRules),
                goalAtom
        );
    }
}
