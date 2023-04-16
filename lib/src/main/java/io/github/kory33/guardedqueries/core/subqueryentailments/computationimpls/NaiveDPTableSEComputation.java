package io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm;
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentComputation;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance;
import io.github.kory33.guardedqueries.core.utils.MappingStreams;
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
        private final ConjunctiveQuery conjunctiveQuery;

        public DPTable(
                final SaturatedRuleSet<? extends NormalGTGD> saturatedRuleSet,
                final FunctionFreeSignature folSignature,
                final ConjunctiveQuery conjunctiveQuery
        ) {
            this.saturatedRuleSet = saturatedRuleSet;
            this.folSignature = folSignature;
            this.conjunctiveQuery = conjunctiveQuery;
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
                // We need to chase the instance with all existential rules
                // while preserving the names in namesToBePreservedDuringChase.
                //
                // To achieve this, we must first injectively map the names in namesToBePreservedDuringChase to
                // universally quantified variables because those values are what get passed down to the chase child.
                // Then we can try to find a substitution that maps unmapped universally quantified variables to
                // local names or constants appearing in the instance.
                final Function<NormalGTGD, Stream<FormalInstance<LocalInstanceTerm>>> allChasesWithRule = (NormalGTGD existentialRule) -> {
                    final var universalVariables = Arrays.asList(existentialRule.getUniversal());

                    // TODO: do something smarter e.g. loop join instead of trying every single possible homomorphism
                    return MappingStreams
                            .allInjectiveTotalFunctionsBetween(namesToBePreservedDuringChase, universalVariables)
                            .flatMap(namesToVariablesInjection -> {
                                final var unmappedVariables = SetLikeExtensions.difference(
                                        universalVariables,
                                        namesToVariablesInjection.values()
                                );

                                final var allMappingsOfUnmappedVariables = MappingStreams.allTotalFunctionsBetween(
                                        unmappedVariables,
                                        instance.getActiveTerms()
                                );

                                final var allHomomorphisms = allMappingsOfUnmappedVariables.map(map ->
                                        ImmutableMapExtensions.union(namesToVariablesInjection.inverse(), map)
                                );

                                final var applicableHomomorphisms = allHomomorphisms.filter(homomorphism -> {
                                    final Stream<FormalFact<LocalInstanceTerm>> mappedBody = Arrays
                                            .stream(existentialRule.getBodyAtoms())
                                            .map(f -> LocalInstanceTermFact.fromAtomWithVariableMap(f, homomorphism::get));

                                    return mappedBody.allMatch(instance::containsFact);
                                });

                                final var usableLocalNamesInChildren = SetLikeExtensions.difference(
                                        IntStream.range(0, folSignature.maxArity() * 2)
                                                .mapToObj(LocalInstanceTerm.LocalName::new)
                                                .toList(),
                                        instance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
                                ).stream().toList();

                                return applicableHomomorphisms.map(homomorphism -> {
                                    final ImmutableMap<Variable, LocalInstanceTerm.LocalName> existentialVariablesMap;
                                    {
                                        final var builder = ImmutableMap.<Variable, LocalInstanceTerm.LocalName>builder();
                                        final var existentialVariables = existentialRule.getExistential();
                                        for (int i = 0; i < existentialVariables.length; i++) {
                                            builder.put(existentialVariables[i], usableLocalNamesInChildren.get(i));
                                        }
                                        existentialVariablesMap = builder.build();
                                    }

                                    final var extendedHomomorphism = ImmutableMapExtensions.union(homomorphism, existentialVariablesMap);

                                    final var headInstance = FormalInstance.fromIterator(
                                            Arrays.stream(existentialRule.getHeadAtoms())
                                                    .map(f -> LocalInstanceTermFact.fromAtomWithVariableMap(f, extendedHomomorphism::get))
                                                    .iterator()
                                    );

                                    final var inherited = instance.restrictToAlphabetsWith(t ->
                                            (t instanceof LocalInstanceTerm.RuleConstant) || (homomorphism.containsValue(t))
                                    );

                                    return datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                                            datalogSaturation,
                                            inherited,
                                            headInstance
                                    );
                                });
                            });
                };

                final Stream<FormalInstance<LocalInstanceTerm>> children = saturatedRuleSet.existentialRules
                        .stream()
                        .flatMap(allChasesWithRule);

                return ImmutableList.copyOf(children.iterator());
            });

            return SetLikeExtensions.generateFromElementsUntilFixpoint(
                    List.of(datalogSaturationEngine.saturateInstance(datalogSaturation, localInstance)),
                    shortcutChaseOneStep
            );
        }

        /**
         * Fill the DP table up to the given instance.
         */
        public void fillTableUpto(final SubqueryEntailmentInstance instance) {
            final var relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                    conjunctiveQuery,
                    instance.coexistentialVariables()
            );

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

                                    final var newRelevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                                            relevantSubquery,
                                            splitCoexistentialVariablesComponent
                                    );

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

                    if (allSplitInstancesAreYesInstances && newlyCoveredAtomsOccurInChasedInstance) {
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
                final var allFormalFactsOverThePredicate = allTotalFunctionsBetween(
                        predicateParameterIndices,
                        allLocalInstanceTerms
                ).map(parameterMap -> {
                    final var parameterList = ImmutableList.<LocalInstanceTerm>copyOf(
                            IntStream
                                    .range(0, predicate.getArity())
                                    .mapToObj(parameterMap::get)
                                    .iterator()
                    );

                    return new FormalFact<LocalInstanceTerm>(predicate, parameterList);
                });

                return () -> SetLikeExtensions
                        .powerset(allFormalFactsOverThePredicate.toList())
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
        final var queryVariables = ImmutableSet.copyOf(conjunctiveQuery.getVariablesRecursive());

        return allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(ruleConstantWitnessGuess ->
                allLocalInstances(signature, ruleConstants).flatMap(localInstance -> {
                    final var allCoexistentialVariableSets = SetLikeExtensions
                            .powerset(queryVariables)
                            .filter(variableSet -> SetLikeExtensions.disjoint(variableSet, ruleConstantWitnessGuess.keySet()))
                            .filter(variableSet -> ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet));

                    return allCoexistentialVariableSets.flatMap(coexistentialVariables -> {
                        final ImmutableSet<Variable> nonConstantNeighbourhood = SetLikeExtensions.difference(
                                ConjunctiveQueryExtensions.neighbourhoodVariables(conjunctiveQuery, coexistentialVariables),
                                ruleConstantWitnessGuess.keySet()
                        );

                        final var allLocalWitnessGuesses = allTotalFunctionsBetween(
                                nonConstantNeighbourhood,
                                localInstance.getActiveTermsInClass(LocalInstanceTerm.LocalName.class)
                        );

                        return allLocalWitnessGuesses.flatMap(localWitnessGuess -> {
                            final var relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                                    conjunctiveQuery, coexistentialVariables
                            );
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
            final ConjunctiveQuery conjunctiveQuery) {
        final var signature = FunctionFreeSignature.encompassingRuleQuery(saturatedRuleSet.allRules, conjunctiveQuery);
        final var ruleConstants = saturatedRuleSet.constants();

        final var dpTable = new DPTable(saturatedRuleSet, signature, conjunctiveQuery);

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
        allWellFormedSubqueryEntailmentInstancesFor(signature, ruleConstants, conjunctiveQuery).forEach(dpTable::fillTableUpto);

        return dpTable.getKnownYesInstances();
    }
}
