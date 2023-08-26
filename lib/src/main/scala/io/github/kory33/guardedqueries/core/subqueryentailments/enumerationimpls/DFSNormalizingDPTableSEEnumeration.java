package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance;
import io.github.kory33.guardedqueries.core.utils.extensions.*;
import org.apache.commons.lang3.tuple.Pair;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.kory33.guardedqueries.core.utils.MappingStreams.*;

/**
 * An implementation of subquery entailment enumeration using a DP table
 * together with efficient DFS traversal and normalization.
 */
public final class DFSNormalizingDPTableSEEnumeration implements SubqueryEntailmentEnumeration {
    private final DatalogSaturationEngine datalogSaturationEngine;

    public DFSNormalizingDPTableSEEnumeration(final DatalogSaturationEngine datalogSaturationEngine) {
        this.datalogSaturationEngine = datalogSaturationEngine;
    }

    private final class DPTable {
        private final HashMap<SubqueryEntailmentInstance, Boolean> table = new HashMap<>();
        private final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet;
        private final FunctionFreeSignature extensionalSignature;
        private final int maxArityOfAllPredicatesUsedInRules;
        private final ConjunctiveQuery connectedConjunctiveQuery;

        public DPTable(
                final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet,
                final FunctionFreeSignature extensionalSignature,
                final int maxArityOfAllPredicatesUsedInRules,
                final ConjunctiveQuery connectedConjunctiveQuery
        ) {
            this.saturatedRuleSet = saturatedRuleSet;
            this.extensionalSignature = extensionalSignature;
            this.maxArityOfAllPredicatesUsedInRules = maxArityOfAllPredicatesUsedInRules;
            this.connectedConjunctiveQuery = connectedConjunctiveQuery;
        }

        private boolean isYesInstance(final SubqueryEntailmentInstance instance) {
            fillTableUpto(instance);
            return this.table.get(instance);
        }

        /**
         * Returns a stream of all children (in the shortcut chase tree) of the given saturated instance
         */
        private Stream<FormalInstance<LocalInstanceTerm>> allNormalizedSaturatedChildrenOf(
                final FormalInstance<LocalInstanceTerm> saturatedInstance,
                final ImmutableSet<LocalInstanceTerm.LocalName> namesToBePreserved
        ) {
            // We need to chase the instance with all existential rules
            // while preserving all names in namesToBePreservedDuringChase.
            //
            // A name is preserved by a chase step if and only if
            // it appears in the substituted head of the existential rule.
            //
            // We can first find all possible homomorphisms from the body of
            // the existential rule to the instance by a join algorithm,
            // and then filter out those that do not preserve the names.
            //
            // NORMALIZATION: unlike in NaiveDPTableSEEnumeration,
            // when we apply an existential rule, we "reuse" local names below
            // maxArityOfAllPredicatesUsedInRules (since we don't care
            // about the identity of local names at all, we can ignore the
            // "direct equivalence" semantics for implicitly-equality-coded
            // tree codes).
            final Function<NormalGTGD, Stream<FormalInstance<LocalInstanceTerm>>> allChasesWithRule = (NormalGTGD existentialRule) -> {
                final var headAtom = existentialRule.getHeadAtoms()[0];

                // A set of existential variables in the existential rule
                final var existentialVariables =
                        ImmutableSet.copyOf(existentialRule.getHead().getBoundVariables());

                final var bodyJoinResult =
                        new FilterNestedLoopJoin<LocalInstanceTerm>(LocalInstanceTerm.RuleConstant::new)
                                .join(TGDExtensions.bodyAsCQ(existentialRule), saturatedInstance);

                // because we are "reusing" local names, we can no longer
                // uniformly extend homomorphisms to existential variables
                // (i.e. local names to which existential variables are mapped depend on
                //  how frontier variables are mapped to local names, as those are the
                //  local names that get inherited to the child instance)
                return bodyJoinResult.allHomomorphisms.stream().flatMap(bodyHomomorphism -> {
                    // The set of local names that are inherited from the parent instance
                    // to the child instance.
                    final var inheritedLocalNames = ImmutableSet.copyOf(
                            TGDExtensions.frontierVariables(existentialRule).stream()
                                    .map(bodyHomomorphism)
                                    .iterator()
                    );

                    // Names we can reuse (i.e. assign to existential variables in the rule)
                    // in the child instance. All names in this set should be considered distinct
                    // from the names in the parent instance having the same value, so we
                    // are explicitly ignoring the "implicit equality coding" semantics here.
                    final var namesToReuseInChild = SetLikeExtensions.difference(
                            IntStream.range(0, maxArityOfAllPredicatesUsedInRules)
                                    .mapToObj(LocalInstanceTerm.LocalName::new)
                                    .toList(),
                            inheritedLocalNames
                    ).asList();

                    final var headVariableHomomorphism = ImmutableMapExtensions.consumeAndCopy(
                            StreamExtensions
                                    .zipWithIndex(existentialVariables.stream())
                                    .map(pair -> {
                                        // for i'th head existential variable, we use namesToReuseInChild(i)
                                        final var variable = pair.getKey();
                                        final var index = pair.getValue().intValue();
                                        final LocalInstanceTerm localName = namesToReuseInChild.get(index);

                                        return Map.<Variable, LocalInstanceTerm>entry(variable, localName);
                                    }).iterator()
                    );

                    final var extendedHomomorphism =
                            bodyHomomorphism.extendWithMapping(headVariableHomomorphism);

                    // The instance containing only the head atom produced by the existential rule.
                    // This should be a singleton instance because the existential rule is normal.
                    final var headInstance = FormalInstance.of(
                            extendedHomomorphism.materializeFunctionFreeAtom(
                                    headAtom, LocalInstanceTerm.RuleConstant::new
                            )
                    );

                    // if names are not preserved, we reject this homomorphism
                    if (!inheritedLocalNames.containsAll(namesToBePreserved)) {
                        return Stream.empty();
                    }

                    // The set of facts in the parent instance that are
                    // "guarded" by the head of the existential rule.
                    // Those are precisely the facts that have its local names
                    // appearing in the head of the existential rule
                    // as a homomorphic image of a frontier variable in the rule.
                    final var inheritedFactsInstance = saturatedInstance.restrictToAlphabetsWith(term ->
                            term.isConstantOrSatisfies(inheritedLocalNames::contains)
                    );

                    // The child instance, which is the saturation of the union of
                    // the set of inherited facts and the head instance.
                    final var childInstance = datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                            saturatedRuleSet.saturatedRulesAsDatalogProgram,
                            // because the parent is saturated, a restriction of it to the alphabet
                            // occurring in the child is also saturated.
                            inheritedFactsInstance,
                            headInstance,
                            LocalInstanceTerm.RuleConstant::new
                    );

                    // we only need to keep chasing with extensional signature
                    return Stream.of(childInstance.restrictToSignature(extensionalSignature));
                });
            };

            return saturatedRuleSet.existentialRules.stream().flatMap(allChasesWithRule);
        }

        /**
         * Check if the given instance (whose relevant subquery is given) can be split into
         * YES instances without chasing further.
         *
         * @param relevantSubquery this must equal {@code ConjunctiveQueryExtensions.subqueryRelevantToVariables(connectedConjunctiveQuery, instance.coexistentialVariables()).get()}
         */
        private boolean canBeSplitIntoYesInstancesWithoutChasing(
                final ConjunctiveQuery relevantSubquery,
                final SubqueryEntailmentInstance instance
        ) {
            final var localWitnessGuessExtensions = allPartialFunctionsBetween(
                    instance.coexistentialVariables(),
                    instance.localInstance().getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
            );

            for (final var localWitnessGuessExtension : StreamExtensions.intoIterableOnce(localWitnessGuessExtensions)) {
                if (localWitnessGuessExtension.isEmpty()) {
                    // we do not allow "empty split"; whenever we split (i.e. make some progress
                    // in the chase automaton), we must pick a nonempty set of coexistential variables
                    // to map to local names in the chased instance.
                    continue;
                }

                final var newlyCoveredVariables = localWitnessGuessExtension.keySet();
                final var extendedLocalWitnessGuess = ImmutableMapExtensions.union(
                        instance.localWitnessGuess(),
                        localWitnessGuessExtension
                );

                final boolean newlyCoveredAtomsOccurInChasedInstance;
                {
                    final var extendedGuess = ImmutableMapExtensions.union(
                            extendedLocalWitnessGuess,
                            instance.ruleConstantWitnessGuessAsMapToInstanceTerms()
                    );
                    final var coveredVariables = extendedGuess.keySet();
                    final var newlyCoveredAtoms = Arrays.stream(relevantSubquery.getAtoms())
                            .filter(atom -> {
                                final var atomVariables = ImmutableSet.copyOf(Arrays.asList(atom.getVariables()));
                                final var allVariablesAreCovered = coveredVariables.containsAll(atomVariables);

                                // we no longer care about the part of the query
                                // which entirely lies in the neighborhood of coexistential variables
                                // of the instance
                                final var someVariableIsNewlyCovered = atomVariables.stream()
                                        .anyMatch(newlyCoveredVariables::contains);

                                return allVariablesAreCovered && someVariableIsNewlyCovered;
                            });

                    newlyCoveredAtomsOccurInChasedInstance = newlyCoveredAtoms
                            .map(atom -> LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess::get))
                            .allMatch(instance.localInstance()::containsFact);
                }

                if (!newlyCoveredAtomsOccurInChasedInstance) {
                    continue;
                }

                final boolean allSplitInstancesAreYesInstances;
                {
                    final ImmutableSet<ImmutableSet<Variable>> splitCoexistentialVariables =
                            ImmutableSet.copyOf(ConjunctiveQueryExtensions.connectedComponents(
                                    relevantSubquery,
                                    SetLikeExtensions.difference(
                                            instance.coexistentialVariables(),
                                            newlyCoveredVariables
                                    )
                            ).iterator());

                    allSplitInstancesAreYesInstances = splitCoexistentialVariables.stream()
                            .allMatch(splitCoexistentialVariablesComponent -> {
                                final var newNeighbourhood = SetLikeExtensions.difference(
                                        ConjunctiveQueryExtensions.neighbourhoodVariables(
                                                relevantSubquery,
                                                splitCoexistentialVariablesComponent
                                        ),
                                        instance.ruleConstantWitnessGuess().keySet()
                                );

                                // For the same reason as .get() call in the beginning of the method,
                                // this .get() call succeeds.
                                //noinspection OptionalGetWithoutIsPresent
                                final var newRelevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                                        relevantSubquery,
                                        splitCoexistentialVariablesComponent
                                ).get();

                                final SubqueryEntailmentInstance inducedInstance = new SubqueryEntailmentInstance(
                                        instance.ruleConstantWitnessGuess(),
                                        splitCoexistentialVariablesComponent,
                                        instance.localInstance(),
                                        MapExtensions.restrictToKeys(
                                                extendedLocalWitnessGuess,
                                                newNeighbourhood
                                        ),
                                        MapExtensions.restrictToKeys(
                                                instance.queryConstantEmbedding(),
                                                ConjunctiveQueryExtensions.constantsIn(newRelevantSubquery)
                                        )
                                );

                                return isYesInstance(inducedInstance);
                            });
                }

                if (allSplitInstancesAreYesInstances) {
                    return true;
                }
            }

            // We tried all possible splits, and none of them worked.
            return false;
        }

        /**
         * Fill the DP table up to the given instance by chasing local instance when necessary.
         */
        private void fillTableUpto(final SubqueryEntailmentInstance instance) {
            if (this.table.containsKey(instance)) {
                return;
            }

            final var saturatedInstance = instance.withLocalInstance(
                    datalogSaturationEngine.saturateInstance(
                            saturatedRuleSet.saturatedRulesAsDatalogProgram,
                            instance.localInstance(),
                            LocalInstanceTerm.RuleConstant::new
                    )
            );

            if (this.table.containsKey(saturatedInstance)) {
                return;
            }

            // The subquery for which we are trying to decide the entailment problem.
            // If the instance is well-formed, the variable set is non-empty and connected,
            // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
            //noinspection OptionalGetWithoutIsPresent
            final var relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                    connectedConjunctiveQuery,
                    saturatedInstance.coexistentialVariables()
            ).get();

            // Check if the root instance can be split
            if (canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery, saturatedInstance)) {
                // we do not need to chase further
                this.table.put(saturatedInstance, true);
                return;
            }

            // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
            // because they are treated as special symbols corresponding to variables and query constants
            // occurring in the subquery.
            final var localNamesToPreserveDuringChase = SetLikeExtensions.union(
                    saturatedInstance.localWitnessGuess().values(),
                    saturatedInstance.queryConstantEmbedding().values()
            );

            // Chasing procedure:
            //   We hold a pair of (parent instance, children iterator) to the stack
            //   and perform a DFS. The children iterator is used to lazily visit
            //   the children of the parent instance. When we have found a YES instance,
            //   we mark all ancestors of the instance as YES. If we exhaust the children
            //   at any one node, we mark the node as NO and move back to the parent.
            //   Every time we chase, we add the chased local instance to localInstancesAlreadySeen
            //   to prevent ourselves from chasing the same instance twice.
            final var stack = new ArrayDeque<Pair<SubqueryEntailmentInstance, Iterator<FormalInstance<LocalInstanceTerm>>>>();
            final var localInstancesAlreadySeen = new HashSet<FormalInstance<LocalInstanceTerm>>();

            // We begin DFS with the root saturated instance.
            stack.add(Pair.of(
                    saturatedInstance,
                    allNormalizedSaturatedChildrenOf(saturatedInstance.localInstance(), localNamesToPreserveDuringChase).iterator()
            ));
            localInstancesAlreadySeen.add(saturatedInstance.localInstance());

            while (!stack.isEmpty()) {
                if (!stack.peekFirst().getRight().hasNext()) {
                    // if we have exhausted the children, we mark the current instance as NO
                    final var currentInstance = stack.pop().getLeft();
                    this.table.put(currentInstance, false);
                    continue;
                }

                // next instance that we wish to decide the entailment problem for
                @SuppressWarnings("DataFlowIssue") // stack is non-empty here
                final var nextChasedInstance = stack.peekFirst().getRight().next();

                // if we have already seen this instance, we do not need to test for entailment
                // (we already know that it is a NO instance)
                if (localInstancesAlreadySeen.contains(nextChasedInstance)) {
                    continue;
                } else {
                    localInstancesAlreadySeen.add(nextChasedInstance);
                }

                final var chasedSubqueryEntailmentInstance = saturatedInstance.withLocalInstance(nextChasedInstance);
                final var weAlreadyVisitedNextInstance = this.table.containsKey(chasedSubqueryEntailmentInstance);

                // if this is an instance we have already seen, we do not need to chase any further
                if (weAlreadyVisitedNextInstance && !this.table.get(chasedSubqueryEntailmentInstance)) {
                    // otherwise we proceed to the next sibling, so continue without modifying the stack
                    continue;
                }

                // If we know that the next instance is a YES instance,
                // or we can split the current instance into YES instances without chasing,
                // we mark all ancestors of the current instance as YES and exit
                if ((weAlreadyVisitedNextInstance && this.table.get(chasedSubqueryEntailmentInstance)) ||
                        canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery, chasedSubqueryEntailmentInstance)) {
                    for (final var ancestor : stack) {
                        this.table.put(ancestor.getLeft(), true);
                    }
                    // we also put the original (unsaturated) instance into the table
                    this.table.put(instance, true);
                    return;
                }

                // It turned out that we cannot split the current instance into YES instances.
                // At this point, we need to chase further down the shortcut chase tree.
                // So we push the chased instance onto the stack and keep on.
                stack.push(Pair.of(
                        chasedSubqueryEntailmentInstance,
                        allNormalizedSaturatedChildrenOf(nextChasedInstance, localNamesToPreserveDuringChase).iterator()
                ));
            }

            // At this point, we have marked the saturated root instance as NO (since we emptied the stack
            // and every time we pop an instance from the stack, we marked that instance as NO).
            // Finally, we mark the original (unsaturated) instance as NO as well.
            this.table.put(instance, false);
        }

        public Stream<SubqueryEntailmentInstance> getKnownYesInstances() {
            return this.table.entrySet().stream()
                    .filter(Map.Entry::getValue)
                    .map(HashMap.Entry::getKey);
        }
    }

    /**
     * Checks whether the given set of local names is of a form {0, ..., n - 1} for some n.
     */
    private static boolean isZeroStartingContiguousLocalNameSet(final ImmutableSet<LocalInstanceTerm.LocalName> localNames) {
        var firstElementAfterZeroNotContainedInSet = 0;
        while (localNames.contains(new LocalInstanceTerm.LocalName(firstElementAfterZeroNotContainedInSet))) {
            firstElementAfterZeroNotContainedInSet++;
        }
        return firstElementAfterZeroNotContainedInSet == localNames.size();
    }

    private static Stream<FormalInstance<LocalInstanceTerm>> allNormalizedLocalInstances(
            final FunctionFreeSignature extensionalSignature,
            final ImmutableSet<Constant> ruleConstants
    ) {
        final var maxArityOfExtensionalSignature = extensionalSignature.maxArity();
        final var ruleConstantsAsLocalTerms = ImmutableSet.copyOf(
                ruleConstants.stream().map(LocalInstanceTerm.RuleConstant::new).iterator()
        );

        // We need to consider sufficiently large collection of set of active local names.
        // As it is sufficient to check subquery entailments for all guarded instance
        // over the extensional signature, and the extensional signature has
        // maxArityOfExtensionalSignature as the maximal arity, we only need to
        // consider a powerset of {0, ..., maxArityOfExtensionalSignature - 1}
        // (NORMALIZATION:
        //  By remapping all local names in the range
        //  [maxArityOfExtensionalSignature, maxArityOfExtensionalSignature * 2)
        //  to the range [0, maxArityOfExtensionalSignature), since the size of
        //  active local name set of local instances necessary to check
        //  is at most maxArityOfExtensionalSignature.
        //  Moreover, by symmetry of instance we can demand that the set of active
        //  names to be contiguous and starting from 0, i.e. {0, ..., n} for some n < maxArityOfExtensionalSignature.
        // )

        final var localNames = ImmutableSet.copyOf(
                IntStream
                        .range(0, maxArityOfExtensionalSignature)
                        .mapToObj(LocalInstanceTerm.LocalName::new)
                        .iterator()
        );

        final var allLocalInstanceTerms =
                SetLikeExtensions.union(localNames, ruleConstantsAsLocalTerms);
        final var predicateList =
                extensionalSignature.predicates().stream().toList();

        final Function<Predicate, Iterable<FormalInstance<LocalInstanceTerm>>> allLocalInstancesOverThePredicate = predicate -> {
            final var predicateParameterIndices = IntStream.range(0, predicate.getArity()).boxed().toList();
            final var allFormalFactsOverThePredicate = ImmutableList.copyOf(
                    allTotalFunctionsBetween(predicateParameterIndices, allLocalInstanceTerms).map(parameterMap -> {
                        final var parameterList = ImmutableList.<LocalInstanceTerm>copyOf(
                                IntStream
                                        .range(0, predicate.getArity())
                                        .mapToObj(parameterMap::get)
                                        .iterator()
                        );

                        //noinspection Convert2Diamond (IDEA fails to infer this)
                        return new FormalFact<LocalInstanceTerm>(predicate, parameterList);
                    }).iterator()
            );

            return () -> SetLikeExtensions
                    .powerset(allFormalFactsOverThePredicate)
                    .map(FormalInstance::new)
                    .iterator();
        };

        final var allInstancesOverLocalNameSet = IteratorExtensions.mapInto(
                ListExtensions
                        .productMappedCollectionsToSets(predicateList, allLocalInstancesOverThePredicate)
                        .iterator(),
                FormalInstance::unionAll
        );

        return IteratorExtensions
                .intoStream(allInstancesOverLocalNameSet)
                .filter(instance -> isZeroStartingContiguousLocalNameSet(
                        instance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
                ));
    }

    private static Stream<SubqueryEntailmentInstance> allWellFormedNormalizedSubqueryEntailmentInstancesFor(
            final FunctionFreeSignature extensionalSignature,
            final ImmutableSet<Constant> ruleConstants,
            final ConjunctiveQuery conjunctiveQuery
    ) {
        final var queryVariables = ConjunctiveQueryExtensions.variablesIn(conjunctiveQuery);
        final var queryExistentialVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());

        return allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(ruleConstantWitnessGuess -> {
            final var allCoexistentialVariableSets = SetLikeExtensions
                    .powerset(queryExistentialVariables)
                    .filter(variableSet -> !variableSet.isEmpty())
                    .filter(variableSet -> SetLikeExtensions.disjoint(variableSet, ruleConstantWitnessGuess.keySet()))
                    .filter(variableSet -> ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet));

            return allCoexistentialVariableSets.flatMap(coexistentialVariables ->
                    allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(localInstance -> {
                        // As coexistentialVariables is a nonempty subset of queryVariables,
                        // we expect to see a non-empty optional.
                        //noinspection OptionalGetWithoutIsPresent
                        final var relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                                conjunctiveQuery, coexistentialVariables
                        ).get();

                        final ImmutableSet<Variable> nonConstantNeighbourhood = SetLikeExtensions.difference(
                                ConjunctiveQueryExtensions.neighbourhoodVariables(conjunctiveQuery, coexistentialVariables),
                                ruleConstantWitnessGuess.keySet()
                        );

                        final var allLocalWitnessGuesses = allTotalFunctionsBetween(
                                nonConstantNeighbourhood,
                                localInstance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
                                        .stream()
                                        .filter(localName -> localName.value() < nonConstantNeighbourhood.size())
                                        .toList()
                        );

                        return allLocalWitnessGuesses.flatMap(localWitnessGuess -> {
                            final var subqueryConstants = SetLikeExtensions.difference(
                                    ConjunctiveQueryExtensions.constantsIn(relevantSubquery),
                                    ruleConstants
                            );
                            final var nonWitnessingActiveLocalNames = SetLikeExtensions.difference(
                                    localInstance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class),
                                    localWitnessGuess.values()
                            );

                            final var allQueryConstantEmbeddings =
                                    allInjectiveTotalFunctionsBetween(subqueryConstants, nonWitnessingActiveLocalNames);

                            return allQueryConstantEmbeddings.map(queryConstantEmbedding -> new SubqueryEntailmentInstance(
                                    ruleConstantWitnessGuess,
                                    coexistentialVariables,
                                    localInstance,
                                    localWitnessGuess,
                                    queryConstantEmbedding
                            ));
                        });
                    })
            );
        });

    }

    @Override
    public Stream<SubqueryEntailmentInstance> apply(
            final FunctionFreeSignature extensionalSignature,
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet,
            final ConjunctiveQuery connectedConjunctiveQuery
    ) {
        final var ruleConstants = saturatedRuleSet.constants();
        final var maxArityOfAllPredicatesUsedInRules = FunctionFreeSignature
                .encompassingRuleQuery(saturatedRuleSet.allRules, connectedConjunctiveQuery)
                .maxArity();

        final var dpTable = new DPTable(
                saturatedRuleSet,
                extensionalSignature,
                maxArityOfAllPredicatesUsedInRules,
                connectedConjunctiveQuery
        );

        allWellFormedNormalizedSubqueryEntailmentInstancesFor(
                extensionalSignature,
                ruleConstants,
                connectedConjunctiveQuery
        ).forEach(dpTable::fillTableUpto);

        return dpTable.getKnownYesInstances();
    }

    @Override
    public String toString() {
        return "DFSNormalizingDPTableSEEnumeration{" +
                "datalogSaturationEngine=" + datalogSaturationEngine +
                '}';
    }
}
