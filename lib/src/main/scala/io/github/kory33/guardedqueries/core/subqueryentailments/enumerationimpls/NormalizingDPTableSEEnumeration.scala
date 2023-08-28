package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import scala.jdk.CollectionConverters.*

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstanceTerm,
  SubqueryEntailmentEnumeration,
  SubqueryEntailmentInstance
}
import io.github.kory33.guardedqueries.core.utils.extensions.*
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import java.util
import java.util.function.Function
import java.util.stream.IntStream
import java.util.stream.Stream
import io.github.kory33.guardedqueries.core.utils.MappingStreams.*
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import com.google.common.collect.ImmutableMap
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact

/**
 * An implementation of subquery entailment enumeration using a DP table plus a simple
 * normalization.
 */
object NormalizingDPTableSEEnumeration {

  /**
   * Checks whether the given set of local names is of a form {0, ..., n - 1} for some n.
   */
  private def isZeroStartingContiguousLocalNameSet(localNames: ImmutableSet[LocalName]) = {
    var firstElementAfterZeroNotContainedInSet = 0
    while (localNames.contains(new LocalName(firstElementAfterZeroNotContainedInSet)))
      firstElementAfterZeroNotContainedInSet += 1
    firstElementAfterZeroNotContainedInSet == localNames.size
  }

  private def allNormalizedLocalInstances(extensionalSignature: FunctionFreeSignature,
                                          ruleConstants: ImmutableSet[Constant]
  ) = {
    val maxArityOfExtensionalSignature = extensionalSignature.maxArity
    val ruleConstantsAsLocalTerms = ImmutableSet.copyOf(
      ruleConstants.stream.map(LocalInstanceTerm.RuleConstant(_)).iterator
    )
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
    val localNames = ImmutableSet.copyOf(IntStream.range(
      0,
      maxArityOfExtensionalSignature
    ).mapToObj(LocalInstanceTerm.LocalName(_)).iterator)

    val allLocalInstanceTerms = SetLikeExtensions.union(localNames, ruleConstantsAsLocalTerms)
    val predicateList = extensionalSignature.predicates.stream.toList
    val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
      val predicateParameterIndices = IntStream.range(0, predicate.getArity).boxed.toList
      val allFormalFactsOverThePredicate = ImmutableList.copyOf(allTotalFunctionsBetween(
        predicateParameterIndices,
        allLocalInstanceTerms
      ).map(parameterMap => {
        val parameterList = ImmutableList.copyOf(IntStream.range(
          0,
          predicate.getArity
        ).mapToObj(parameterMap.get).iterator)

        FormalFact(predicate, parameterList)
      }).iterator)

      (
        () =>
          SetLikeExtensions.powerset(allFormalFactsOverThePredicate).map(
            FormalInstance(_)
          ).iterator
      ): java.lang.Iterable[FormalInstance[LocalInstanceTerm]]
    }

    val allInstancesOverLocalNameSet = IteratorExtensions.mapInto(
      ListExtensions.productMappedCollectionsToSets(
        predicateList,
        allLocalInstancesOverThePredicate
      ).iterator,
      FormalInstance.unionAll
    )

    IteratorExtensions.intoStream(allInstancesOverLocalNameSet).filter(instance =>
      isZeroStartingContiguousLocalNameSet(instance.getActiveTermsInClass(classOf[LocalName]))
    )
  }
  private def allWellFormedNormalizedSubqueryEntailmentInstancesFor(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: ImmutableSet[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ) = {
    val queryVariables = ConjunctiveQueryExtensions.variablesIn(conjunctiveQuery)
    val queryExistentialVariables = ImmutableSet.copyOf(conjunctiveQuery.getBoundVariables)
    allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: ImmutableMap[Variable, Constant]) => {

        val allCoexistentialVariableSets =
          SetLikeExtensions.powerset(queryExistentialVariables).filter(
            (variableSet: ImmutableSet[Variable]) => !variableSet.isEmpty
          ).filter((variableSet: ImmutableSet[Variable]) =>
            SetLikeExtensions.disjoint(variableSet, ruleConstantWitnessGuess.keySet)
          ).filter((variableSet: ImmutableSet[Variable]) =>
            ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet)
          )

        allCoexistentialVariableSets.flatMap((coexistentialVariables: ImmutableSet[Variable]) =>
          allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(
            (localInstance: FormalInstance[LocalInstanceTerm]) => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                conjunctiveQuery,
                coexistentialVariables
              ).get

              val nonConstantNeighbourhood = SetLikeExtensions.difference(
                ConjunctiveQueryExtensions.neighbourhoodVariables(
                  conjunctiveQuery,
                  coexistentialVariables
                ),
                ruleConstantWitnessGuess.keySet
              )

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance.getActiveTermsInClass(classOf[LocalName]).stream.filter(
                  (localName) => localName.value < nonConstantNeighbourhood.size
                ).toList
              )
              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants = SetLikeExtensions.difference(
                  ConjunctiveQueryExtensions.constantsIn(relevantSubquery),
                  ruleConstants
                )
                val nonWitnessingActiveLocalNames = SetLikeExtensions.difference(
                  localInstance.getActiveTermsInClass(classOf[LocalName]),
                  localWitnessGuess.values
                )
                val allQueryConstantEmbeddings = allInjectiveTotalFunctionsBetween(
                  subqueryConstants,
                  nonWitnessingActiveLocalNames
                )
                allQueryConstantEmbeddings.map(queryConstantEmbedding =>
                  new SubqueryEntailmentInstance(
                    ruleConstantWitnessGuess,
                    coexistentialVariables,
                    localInstance,
                    localWitnessGuess,
                    queryConstantEmbedding
                  )
                )

              })

            }
          )
        )

      }
    )
  }
}

final class NormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {
    final private val table = new util.HashMap[SubqueryEntailmentInstance, Boolean]

    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      if (!this.table.containsKey(instance)) fillTableUpto(instance)
      this.table.get(instance)
    }

    private def chaseLocalInstance(localInstance: FormalInstance[LocalInstanceTerm],
                                   namesToBePreservedDuringChase: ImmutableSet[LocalName]
    ) = {
      val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram
      val shortcutChaseOneStep =
        FunctionExtensions.asFunction((instance: FormalInstance[LocalInstanceTerm]) => {
          def foo(instance: FormalInstance[LocalInstanceTerm]) = {
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
            val allChasesWithRule = (existentialRule: NormalGTGD) => {
              def foo(existentialRule: NormalGTGD) = {
                val headAtom = existentialRule.getHeadAtoms()(0)

                // A set of existential variables in the existential rule
                val existentialVariables =
                  ImmutableSet.copyOf(existentialRule.getHead.getBoundVariables)
                val bodyJoinResult = new FilterNestedLoopJoin[LocalInstanceTerm](
                  LocalInstanceTerm.RuleConstant(_)
                ).join(
                  TGDExtensions.bodyAsCQ(existentialRule),
                  instance
                )

                // because we are "reusing" local names, we can no longer
                // uniformly extend homomorphisms to existential variables
                // (i.e. local names to which existential variables are mapped depend on
                //  how frontier variables are mapped to local names, as those are the
                //  local names that get inherited to the child instance)
                bodyJoinResult.allHomomorphisms.stream.flatMap((bodyHomomorphism) => {
                  def foo(bodyHomomorphism: HomomorphicMapping[LocalInstanceTerm])
                    : Stream[FormalInstance[LocalInstanceTerm]] = {

                    // The set of local names that are inherited from the parent instance
                    // to the child instance.
                    val inheritedLocalNames =
                      ImmutableSet.copyOf(TGDExtensions.frontierVariables(
                        existentialRule
                      ).stream.map(bodyHomomorphism).iterator)

                    // Names we can reuse (i.e. assign to existential variables in the rule)
                    // in the child instance. All names in this set should be considered distinct
                    // from the names in the parent instance having the same value, so we
                    // are explicitly ignoring the "implicit equality coding" semantics here.
                    val namesToReuseInChild = SetLikeExtensions.difference(
                      IntStream.range(0, maxArityOfAllPredicatesUsedInRules).mapToObj(
                        LocalInstanceTerm.LocalName(_)
                      ).toList,
                      inheritedLocalNames
                    ).asList

                    val headVariableHomomorphism = ImmutableMapExtensions.consumeAndCopy(
                      StreamExtensions.zipWithIndex(existentialVariables.stream).map(pair => {
                        // for i'th head existential variable, we use namesToReuseInChild(i)
                        val variable = pair.getKey
                        val index = pair.getValue.intValue
                        val localName = namesToReuseInChild.get(index)
                        util.Map.entry(variable, localName)
                      }).iterator
                    )

                    val extendedHomomorphism =
                      bodyHomomorphism.extendWithMapping(headVariableHomomorphism)

                    // The instance containing only the head atom produced by the existential rule.
                    // This should be a singleton instance because the existential rule is normal.
                    val headInstance =
                      FormalInstance.of(extendedHomomorphism.materializeFunctionFreeAtom(
                        headAtom,
                        LocalInstanceTerm.RuleConstant(_)
                      ))

                    // if names are not preserved, we reject this homomorphism
                    if (!inheritedLocalNames.containsAll(namesToBePreservedDuringChase))
                      return Stream.empty

                    // The set of facts in the parent instance that are
                    // "guarded" by the head of the existential rule.
                    // Those are precisely the facts that have its local names
                    // appearing in the head of the existential rule
                    // as a homomorphic image of a frontier variable in the rule.
                    val inheritedFactsInstance = instance.restrictToAlphabetsWith((term) =>
                      term.isConstantOrSatisfies(inheritedLocalNames.contains)
                    )

                    // The child instance, which is the saturation of the union of
                    // the set of inherited facts and the head instance.
                    val childInstance =
                      datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                        datalogSaturation,
                        // because the parent is saturated, a restriction of it to the alphabet
                        // occurring in the child is also saturated.
                        inheritedFactsInstance,
                        headInstance,
                        LocalInstanceTerm.RuleConstant(_)
                      )

                    // we only need to keep chasing with extensional signature
                    Stream.of(childInstance.restrictToSignature(extensionalSignature))
                  }
                  foo(bodyHomomorphism)
                })
              }
              foo(existentialRule)
            }

            val children =
              saturatedRuleSet.existentialRules.stream.flatMap(allChasesWithRule(_))

            ImmutableList.copyOf(children.iterator)
          }
          foo(instance)
        })

      // we keep chasing until we reach a fixpoint
      SetLikeExtensions.generateFromElementsUntilFixpoint(
        util.List.of(datalogSaturationEngine.saturateInstance(
          datalogSaturation,
          localInstance,
          LocalInstanceTerm.RuleConstant(_)
        )),
        shortcutChaseOneStep(_)
      )
    }

    /**
     * Fill the DP table up to the given instance.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = {
      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
        connectedConjunctiveQuery,
        instance.coexistentialVariables
      ).get
      val instancesWithGuessedVariablesPreserved = chaseLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        SetLikeExtensions.union(
          instance.localWitnessGuess.values,
          instance.queryConstantEmbedding.values
        )
      )

      for (chasedInstance <- instancesWithGuessedVariablesPreserved.asScala) {
        val localWitnessGuessExtensions = allPartialFunctionsBetween(
          instance.coexistentialVariables,
          chasedInstance.getActiveTermsInClass(classOf[LocalName])
        )

        for (
          localWitnessGuessExtension <-
            StreamExtensions.intoIterableOnce(localWitnessGuessExtensions).asScala
        ) {
          import scala.util.boundary

          boundary:
            if (localWitnessGuessExtension.isEmpty) {
              // we do not allow "empty split"; whenever we split (i.e. make some progress
              // in the chase automaton), we must pick a nonempty set of coexistential variables
              // to map to local names in the chased instance.
              boundary.break()
            }
            val newlyCoveredVariables = localWitnessGuessExtension.keySet
            val extendedLocalWitnessGuess =
              ImmutableMapExtensions.union(
                instance.localWitnessGuess,
                localWitnessGuessExtension
              )

            val newlyCoveredAtomsOccurInChasedInstance = {
              val extendedGuess = ImmutableMapExtensions.union(
                extendedLocalWitnessGuess,
                instance.ruleConstantWitnessGuessAsMapToInstanceTerms
              )
              val coveredVariables = extendedGuess.keySet
              val newlyCoveredAtoms =
                util.Arrays.stream(relevantSubquery.getAtoms).filter((atom: Atom) => {
                  val atomVariables = ImmutableSet.copyOf(util.Arrays.asList(atom.getVariables))
                  val allVariablesAreCovered = coveredVariables.containsAll(atomVariables)
                  // we no longer care about the part of the query
                  // which entirely lies in the neighborhood of coexistential variables
                  // of the instance
                  val someVariableIsNewlyCovered =
                    atomVariables.stream.anyMatch(newlyCoveredVariables.contains)
                  allVariablesAreCovered && someVariableIsNewlyCovered

                })

              newlyCoveredAtoms.map((atom: Atom) =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess.get)
              ).allMatch(chasedInstance.containsFact)
            }

            if (!newlyCoveredAtomsOccurInChasedInstance)
              boundary.break()

            val allSplitInstancesAreYesInstances = {
              val splitCoexistentialVariables =
                ImmutableSet.copyOf(ConjunctiveQueryExtensions.connectedComponents(
                  relevantSubquery,
                  SetLikeExtensions.difference(
                    instance.coexistentialVariables,
                    newlyCoveredVariables
                  )
                ).iterator)

              splitCoexistentialVariables.stream.allMatch(
                (splitCoexistentialVariablesComponent: ImmutableSet[Variable]) => {
                  val newNeighbourhood = SetLikeExtensions.difference(
                    ConjunctiveQueryExtensions.neighbourhoodVariables(
                      relevantSubquery,
                      splitCoexistentialVariablesComponent
                    ),
                    instance.ruleConstantWitnessGuess.keySet
                  )
                  // For the same reason as .get() call in the beginning of the method,
                  // this .get() call succeeds.
                  // noinspection OptionalGetWithoutIsPresent
                  val newRelevantSubquery =
                    ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                      relevantSubquery,
                      splitCoexistentialVariablesComponent
                    ).get
                  val inducedInstance = new SubqueryEntailmentInstance(
                    instance.ruleConstantWitnessGuess,
                    splitCoexistentialVariablesComponent,
                    chasedInstance,
                    MapExtensions.restrictToKeys(extendedLocalWitnessGuess, newNeighbourhood),
                    MapExtensions.restrictToKeys(
                      instance.queryConstantEmbedding,
                      ConjunctiveQueryExtensions.constantsIn(newRelevantSubquery)
                    )
                  )

                  isYesInstance(inducedInstance)
                }
              )
            }

            if (allSplitInstancesAreYesInstances) {
              this.table.put(instance, true)
              return
            }
        }
      }

      // all instances chased from the original instance fail to fulfill the subquery
      // strongly induced by instance.coexistentialVariables(), so we mark the original instance false.
      this.table.put(instance, false)
    }

    def getKnownYesInstances: Stream[SubqueryEntailmentInstance] =
      this.table.entrySet.stream.filter(_.getValue).map(_.getKey)
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): Stream[SubqueryEntailmentInstance] = {
    val ruleConstants = saturatedRuleSet.constants
    val maxArityOfAllPredicatesUsedInRules = FunctionFreeSignature.encompassingRuleQuery(
      saturatedRuleSet.allRules,
      connectedConjunctiveQuery
    ).maxArity

    val dpTable = new DPTable(
      saturatedRuleSet,
      extensionalSignature,
      maxArityOfAllPredicatesUsedInRules,
      connectedConjunctiveQuery
    )

    NormalizingDPTableSEEnumeration.allWellFormedNormalizedSubqueryEntailmentInstancesFor(
      extensionalSignature,
      ruleConstants,
      connectedConjunctiveQuery
    ).forEach(dpTable.fillTableUpto(_))

    dpTable.getKnownYesInstances
  }
  override def toString: String =
    "NormalizingDPTableSEEnumeration{" + "datalogSaturationEngine=" + datalogSaturationEngine + '}'
}
