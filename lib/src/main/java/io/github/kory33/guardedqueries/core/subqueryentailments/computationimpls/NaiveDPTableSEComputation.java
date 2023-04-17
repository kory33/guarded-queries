package io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentComputation;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance;
import io.github.kory33.guardedqueries.core.utils.extensions.*;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.kory33.guardedqueries.core.utils.MappingStreams.*;

public final class NaiveDPTableSEComputation implements SubqueryEntailmentComputation {
    private final DatalogSaturationEngine datalogSaturationEngine;

    public NaiveDPTableSEComputation(final DatalogSaturationEngine datalogSaturationEngine) {
        this.datalogSaturationEngine = datalogSaturationEngine;
    }

    private final class DPTable {
        private final HashMap<SubqueryEntailmentInstance, Boolean> table = new HashMap<>();
        private final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet;
        private final FunctionFreeSignature folSignature;
        private final ConjunctiveQuery connectedConjunctiveQuery;

        public DPTable(
                final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet,
                final FunctionFreeSignature folSignature,
                final ConjunctiveQuery connectedConjunctiveQuery
        ) {
            this.saturatedRuleSet = saturatedRuleSet;
            this.folSignature = folSignature;
            this.connectedConjunctiveQuery = connectedConjunctiveQuery;
        }

        private boolean isYesInstance(final SubqueryEntailmentInstance instance) {
            if (!this.table.containsKey(instance)) {
                fillTableUpto(instance);
            }
            return this.table.get(instance);
        }

        private ImmutableSet<FormalInstance<LocalInstanceTerm>> chaseLocalInstance(
                final FormalInstance<LocalInstanceTerm> localInstance,
                final ImmutableSet<LocalInstanceTerm.LocalName> namesToBePreservedDuringChase
        ) {
            final var datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram;

            final var shortcutChaseOneStep = FunctionExtensions.asFunction((FormalInstance<LocalInstanceTerm> instance) -> {
                final var localNamesUsableInChildren = ImmutableList.copyOf(
                        SetLikeExtensions.difference(
                                IntStream.range(0, folSignature.maxArity() * 2)
                                        .mapToObj(LocalInstanceTerm.LocalName::new)
                                        .toList(),
                                instance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
                        ).stream().iterator()
                );

                // We need to chase the instance with all existential rules
                // while preserving the names in namesToBePreservedDuringChase.
                //
                // For a chase step to preserve names, it must injectively map the
                // names in namesToBePreservedDuringChase to universally quantified variables
                // because those values are what get passed down to the chase child.
                //
                // We can first find all possible homomorphisms from the body of
                // the existential rule to the instance by a join algorithm,
                // and then filter out those that do not preserve the names.
                final Function<NormalGTGD, Stream<FormalInstance<LocalInstanceTerm>>> allChasesWithRule = (NormalGTGD existentialRule) -> {
                    final var bodyHomomorphisms = new FilterNestedLoopJoin<LocalInstanceTerm>().join(
                            TGDExtensions.bodyAsCQ(existentialRule),
                            instance
                    );

                    // An ordering of variables that appear in the body of the existential rule,
                    // as specified by the join algorithm
                    final var variableOrdering = bodyHomomorphisms.variableOrdering();

                    // An ordering of variables that appear in the head of the existential rule
                    final var orderedHeadVariables =
                            ImmutableList.copyOf(ImmutableSet.copyOf(existentialRule.getHead().getBoundVariables()));

                    // An ordering of all variables that appear in the existential rule
                    final var extendedVariableOrdering = ImmutableList.<Variable>builder()
                            .addAll(variableOrdering)
                            .addAll(orderedHeadVariables)
                            .build();

                    // An assignment of names to the variables in the head
                    final var headVariableHomomorphism = ImmutableList.copyOf(
                            IntStream.range(0, orderedHeadVariables.size())
                                    .mapToObj(localNamesUsableInChildren::get)
                                    .iterator()
                    );

                    return bodyHomomorphisms.allHomomorphisms().stream().flatMap(homomorphism -> {
                        // We need to first check that all names in namesToBePreservedDuringChase
                        // appear in the homomorphism exactly once.
                        for (final var name : namesToBePreservedDuringChase) {
                            final var nameCount = homomorphism.stream().filter(term -> term.equals(name)).count();
                            if (nameCount != 1) {
                                return Stream.empty();
                            }
                        }

                        final var extendedHomomorphism = ImmutableList.<LocalInstanceTerm>builder()
                                .addAll(homomorphism)
                                .addAll(headVariableHomomorphism)
                                .build();

                        // the set of facts in the parent instance that are
                        // "guarded" by the head of the existential rule
                        final var inheritedFactsInstance = instance.restrictToAlphabetsWith(t ->
                                (t instanceof LocalInstanceTerm.RuleConstant) || (homomorphism.contains(t))
                        );

                        // The instance containing only the head atom produced by the existential rule.
                        // This should be a singleton instance because the existential rule is normal.
                        final var headInstance = new FormalInstance<>(
                                Arrays.stream(existentialRule.getHeadAtoms())
                                        .map(FormalFact::fromAtom)
                                        .map(fact -> fact.map(term ->
                                                LocalInstanceTerm.fromTermWithVariableMap(term, variable ->
                                                        extendedHomomorphism.get(extendedVariableOrdering.indexOf(variable))
                                                )
                                        ))
                                        .iterator()
                        );

                        // The child instance, which is the saturation of the union of
                        // the set of inherited facts and the head instance.
                        final var childInstance = datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                                datalogSaturation,
                                // because the parent is saturated, a restriction of it to the alphabet
                                // occurring in the child is also saturated.
                                inheritedFactsInstance,
                                headInstance,
                                LocalInstanceTerm.RuleConstant::new
                        );

                        return Stream.of(childInstance);
                    });
                };

                final Stream<FormalInstance<LocalInstanceTerm>> children = saturatedRuleSet.existentialRules
                        .stream()
                        .flatMap(allChasesWithRule);

                return ImmutableList.copyOf(children.iterator());
            });

            // we keep chasing until we reach a fixpoint
            return SetLikeExtensions.generateFromElementsUntilFixpoint(
                    List.of(datalogSaturationEngine.saturateInstance(
                            datalogSaturation,
                            localInstance,
                            LocalInstanceTerm.RuleConstant::new
                    )),
                    shortcutChaseOneStep
            );
        }

        /**
         * Fill the DP table up to the given instance.
         */
        private void fillTableUpto(final SubqueryEntailmentInstance instance) {
            // The subquery for which we are trying to decide the entailment problem.
            // If the instance is well-formed, the variable set is non-empty and connected,
            // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
            //noinspection OptionalGetWithoutIsPresent
            final var relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                    connectedConjunctiveQuery,
                    instance.coexistentialVariables()
            ).get();

            final ImmutableSet<FormalInstance<LocalInstanceTerm>> instancesWithGuessedVariablesPreserved =
                    chaseLocalInstance(
                            instance.localInstance(),
                            // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
                            // because they are treated as special symbols corresponding to variables and query constants
                            // occurring in the subquery.
                            SetLikeExtensions.union(
                                    instance.localWitnessGuess().values(),
                                    instance.queryConstantEmbedding().values()
                            )
                    );

            for (final var chasedInstance : instancesWithGuessedVariablesPreserved) {
                final var localWitnessGuessExtensions = allPartialFunctionsBetween(
                        instance.coexistentialVariables(),
                        chasedInstance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
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
                                .allMatch(chasedInstance::containsFact);
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
                                            chasedInstance,
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
                        this.table.put(instance, true);
                        return;
                    }
                }
            }

            // all instances chased from the original instance fail to fulfill the subquery
            // strongly induced by instance.coexistentialVariables(), so we mark the original instance false.
            this.table.put(instance, false);
        }

        public Stream<SubqueryEntailmentInstance> getKnownYesInstances() {
            return this.table.entrySet().stream()
                    .filter(Map.Entry::getValue)
                    .map(HashMap.Entry::getKey);
        }
    }

    private static Stream<FormalInstance<LocalInstanceTerm>> allLocalInstances(
            final FunctionFreeSignature signature,
            final ImmutableSet<Constant> ruleConstants
    ) {
        final var maxArity = signature.maxArity();
        final var ruleConstantsAsLocalTerms = ImmutableSet.copyOf(
                ruleConstants.stream().map(LocalInstanceTerm.RuleConstant::new).iterator()
        );

        final var allActiveLocalNames = SetLikeExtensions
                .powerset(IntStream.range(0, maxArity * 2).boxed().toList())
                .filter(localNameSet -> localNameSet.size() <= maxArity);

        return allActiveLocalNames.flatMap(localNameSet -> {
            final var localNames = ImmutableSet.copyOf(
                    localNameSet.stream().map(LocalInstanceTerm.LocalName::new).iterator()
            );
            final var allLocalInstanceTerms = SetLikeExtensions.union(localNames, ruleConstantsAsLocalTerms);
            final var predicateList = signature.predicates().stream().toList();

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

                            return new FormalFact<>(predicate, parameterList);
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
                    .filter(instance -> {
                        final var activeLocalNames = instance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class);
                        return activeLocalNames.size() == localNameSet.size();
                    });
        });
    }

    private static Stream<SubqueryEntailmentInstance> allWellFormedSubqueryEntailmentInstancesFor(
            final FunctionFreeSignature signature,
            final ImmutableSet<Constant> ruleConstants,
            final ConjunctiveQuery conjunctiveQuery
    ) {
        final var queryVariables = ConjunctiveQueryExtensions.variablesIn(conjunctiveQuery);
        final var queryExistentialVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables());

        return allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(ruleConstantWitnessGuess ->
                allLocalInstances(signature, ruleConstants).flatMap(localInstance -> {
                    final var allCoexistentialVariableSets = SetLikeExtensions
                            .powerset(queryExistentialVariables)
                            .filter(variableSet -> !variableSet.isEmpty())
                            .filter(variableSet -> SetLikeExtensions.disjoint(variableSet, ruleConstantWitnessGuess.keySet()))
                            .filter(variableSet -> ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet));

                    return allCoexistentialVariableSets.flatMap(coexistentialVariables -> {
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
                        );

                        return allLocalWitnessGuesses.flatMap(localWitnessGuess -> {
                            final var subqueryConstants = ConjunctiveQueryExtensions.constantsIn(relevantSubquery);
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
                    });
                })
        );
    }

    @Override
    public Stream<SubqueryEntailmentInstance> apply(
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet,
            final ConjunctiveQuery connectedConjunctiveQuery) {
        final var signature =
                FunctionFreeSignature.encompassingRuleQuery(saturatedRuleSet.allRules, connectedConjunctiveQuery);
        final var ruleConstants = saturatedRuleSet.constants();

        final var dpTable = new DPTable(saturatedRuleSet, signature, connectedConjunctiveQuery);

        // NOTE:
        //   This algorithm is massively inefficient as-is.
        //   Here are a few optimization points that we could further explore:
        //    - Problem 1:
        //        We actually only need to consider local instances that are
        //        (1) saturated by the input ruleset and (2) guarded as an instance.
        //        The implementation in this class brute-forces all possible local instances,
        //        so we are simply exploring a much larger search space than necessary.
        //    - Problem 2:
        //        Once we mark a problem instance as a true instance, we no longer have to fill the table
        //        for "larger" local instances due to the subsumption.
        //        It is easy to see that, for problems instances `sqei1` and `sqei2`, if
        //         - `sqei2.coexistentialVariables` is a superset of `sqei1.coexistentialVariables`
        //         - there exists a function θ (a "matching" between local names) that sends active local names
        //           in `sqei1.localInstance` to active terms (so either local names or rule constants)
        //           in `sqei2.localInstance`, such that
        //           - `θ(sqei1.localInstance)` is a subinstance of `sqei2.localInstance`
        //           - all of `ruleConstantWitnessGuess`, `localWitnessGuess`, and `queryConstantEmbedding`
        //             of `sqei1` are "matched" to `sqei2` by θ (TODO: make this precise?)
        //        then `sqei1` being a true instance implies `sqei2` being a true instance.
        //    - Problem 3:
        //        During the chase phase, we can always "normalize" local instances so that active values are
        //        always within the range of {0,1,...,maxArity-1}. Moreover, we can rearrange
        //        the local names in the local instance so that the guard atom (which is chosen according
        //        to a canonical order on the predicate names) has its local-name parameters in the increasing
        //        order. This way, we can identify a number of local instances that have the "same shape",
        //        avoiding the need to explore all possible local instances.
        //    - Problem 4:
        //        During the chase phase, if we happen to mark the root problem instance as a true instance,
        //        we can mark all "intermediate" local instances between the root and the successful branching point
        //        as true, too. On the other hand, if we happen to mark the root problem instance as a false instance,
        //        we can mark all local instances below the root as false, too.
        //        The implementation in this class completely ignores this aspect of the tree-structure of the chase.
        //
        //    The challenge to solve these problems essentially boils down to
        //     - keeping track of only "maximally subsuming true instances" and "minimally subsuming false instances"
        //     - efficiently matching a problem instance to other subsuming instances using indexing techniques
        //    which we shall explore in another implementation.
        allWellFormedSubqueryEntailmentInstancesFor(
                signature,
                ruleConstants,
                connectedConjunctiveQuery
        ).forEach(dpTable::fillTableUpto);

        return dpTable.getKnownYesInstances();
    }
}
