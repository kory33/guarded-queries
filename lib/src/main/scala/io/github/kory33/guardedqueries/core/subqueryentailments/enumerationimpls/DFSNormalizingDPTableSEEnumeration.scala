package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  SubqueryEntailmentEnumeration,
  SubqueryEntailmentInstance
}
import io.github.kory33.guardedqueries.core.utils.extensions.*
import org.apache.commons.lang3.tuple.Pair
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import scala.jdk.CollectionConverters._
import java.util.*
import java.util.function.Function
import java.util.stream.IntStream
import java.util.stream.Stream
import io.github.kory33.guardedqueries.core.utils.MappingStreams.*
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import com.google.common.collect.ImmutableMap
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.RuleConstant
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact

/**
 * An implementation of subquery entailment enumeration using a DP table together with efficient
 * DFS traversal and normalization.
 */
object DFSNormalizingDPTableSEEnumeration {

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
    val predicateList = extensionalSignature.predicates.toList
    val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
      val predicateParameterIndices = IntStream.range(0, predicate.getArity).boxed.toList
      val allFormalFactsOverThePredicate = ImmutableList.copyOf(allTotalFunctionsBetween(
        predicateParameterIndices,
        allLocalInstanceTerms
      ).map(parameterMap => {
        val parameterList = ImmutableList.copyOf(
          IntStream.range(0, predicate.getArity).mapToObj(parameterMap.get).iterator
        )
        FormalFact[LocalInstanceTerm](predicate, parameterList)
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
        predicateList.asJava,
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
    allPartialFunctionsBetween(queryVariables.asJava, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: ImmutableMap[Variable, Constant]) => {
        val allCoexistentialVariableSets =
          SetLikeExtensions.powerset(queryExistentialVariables).filter(
            (variableSet: ImmutableSet[Variable]) => !variableSet.isEmpty
          ).filter((variableSet: ImmutableSet[Variable]) =>
            SetLikeExtensions.disjoint(variableSet, ruleConstantWitnessGuess.keySet)
          ).filter((variableSet: ImmutableSet[Variable]) =>
            ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet.asScala.toSet)
          )

        allCoexistentialVariableSets.flatMap((coexistentialVariables: ImmutableSet[Variable]) =>
          allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(
            localInstance => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery =
                ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                  coexistentialVariables.asScala.toSet
                )(
                  conjunctiveQuery
                ).get

              val nonConstantNeighbourhood =
                ConjunctiveQueryExtensions.neighbourhoodVariables(
                  conjunctiveQuery,
                  coexistentialVariables.asScala.toSet
                ) -- ruleConstantWitnessGuess.keySet.asScala.toSet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood.asJava,
                localInstance.getActiveTermsInClass(classOf[LocalName]).stream.filter(
                  (localName) => localName.value < nonConstantNeighbourhood.size
                ).toList
              )

              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants =
                  ConjunctiveQueryExtensions.constantsIn(
                    relevantSubquery
                  ) -- ruleConstants.asScala

                val nonWitnessingActiveLocalNames = SetLikeExtensions.difference(
                  localInstance.getActiveTermsInClass(classOf[LocalName]),
                  localWitnessGuess.values
                )
                val allQueryConstantEmbeddings = allInjectiveTotalFunctionsBetween(
                  subqueryConstants.asJava,
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

final class DFSNormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {

  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {
    final private val table = new java.util.HashMap[SubqueryEntailmentInstance, Boolean]()
    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      fillTableUpto(instance)
      this.table.get(instance)
    }

    /**
     * Returns a stream of all children (in the shortcut chase tree) of the given saturated
     * instance
     */
    private def allNormalizedSaturatedChildrenOf(
      saturatedInstance: FormalInstance[LocalInstanceTerm],
      namesToBePreserved: ImmutableSet[LocalName]
    ) = {
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
          val bodyJoinResult =
            new FilterNestedLoopJoin[LocalInstanceTerm](RuleConstant(_)).join(
              TGDExtensions.bodyAsCQ(existentialRule),
              saturatedInstance
            )

          // because we are "reusing" local names, we can no longer
          // uniformly extend homomorphisms to existential variables
          // (i.e. local names to which existential variables are mapped depend on
          //  how frontier variables are mapped to local names, as those are the
          //  local names that get inherited to the child instance)
          bodyJoinResult.allHomomorphisms.stream.flatMap(bodyHomomorphism => {
            def foo(bodyHomomorphism: HomomorphicMapping[LocalInstanceTerm])
              : Stream[FormalInstance[LocalInstanceTerm]] = {
              // The set of local names that are inherited from the parent instance
              // to the child instance.
              val inheritedLocalNames =
                TGDExtensions.frontierVariables(existentialRule).map(bodyHomomorphism.apply)

              // Names we can reuse (i.e. assign to existential variables in the rule)
              // in the child instance. All names in this set should be considered distinct
              // from the names in the parent instance having the same value, so we
              // are explicitly ignoring the "implicit equality coding" semantics here.
              val namesToReuseInChild =
                (0 until maxArityOfAllPredicatesUsedInRules).map(LocalName(_))
                  .toSet.removedAll(inheritedLocalNames)
                  .toList

              val headVariableHomomorphism = ImmutableMapExtensions.consumeAndCopy(
                StreamExtensions.zipWithIndex(existentialVariables.stream).map((pair) => {
                  // for i'th head existential variable, we use namesToReuseInChild(i)
                  val variable = pair.getKey
                  val index = pair.getValue.intValue
                  val localName = namesToReuseInChild(index)
                  java.util.Map.entry[Variable, LocalInstanceTerm](variable, localName)
                }).iterator
              )

              val extendedHomomorphism =
                bodyHomomorphism.extendWithMapping(headVariableHomomorphism)
                // The instance containing only the head atom produced by the existential rule.
                // This should be a singleton instance because the existential rule is normal.
              val headInstance =
                FormalInstance.of(extendedHomomorphism.materializeFunctionFreeAtom(
                  headAtom,
                  RuleConstant(_)
                ))

              // if names are not preserved, we reject this homomorphism
              if (!namesToBePreserved.asScala.toSet.subsetOf(inheritedLocalNames))
                return Stream.empty

              // The set of facts in the parent instance that are
              // "guarded" by the head of the existential rule.
              // Those are precisely the facts that have its local names
              // appearing in the head of the existential rule
              // as a homomorphic image of a frontier variable in the rule.
              val inheritedFactsInstance = saturatedInstance.restrictToAlphabetsWith((term) =>
                term.isConstantOrSatisfies(inheritedLocalNames.contains)
              )

              // The child instance, which is the saturation of the union of
              // the set of inherited facts and the head instance.
              val childInstance =
                datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                  saturatedRuleSet.saturatedRulesAsDatalogProgram,
                  // because the parent is saturated, a restriction of it to the alphabet
                  // occurring in the child is also saturated.
                  inheritedFactsInstance,
                  headInstance,
                  RuleConstant(_)
                )
              // we only need to keep chasing with extensional signature
              Stream.of(childInstance.restrictToSignature(extensionalSignature))
            }
            foo(bodyHomomorphism)
          })
        }
        foo(existentialRule)
      }
      saturatedRuleSet.existentialRules.stream.flatMap(allChasesWithRule(_))
    }

    /**
     * Check if the given instance (whose relevant subquery is given) can be split into YES
     * instances without chasing further.
     *
     * @param relevantSubquery
     *   this must equal {@code
     *   ConjunctiveQueryExtensions.subqueryRelevantToVariables(connectedConjunctiveQuery,
     *   instance.coexistentialVariables()).get()}
     */
    private def canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery: ConjunctiveQuery,
                                                         instance: SubqueryEntailmentInstance
    ): Boolean = {
      val localWitnessGuessExtensions = allPartialFunctionsBetween(
        instance.coexistentialVariables,
        instance.localInstance.getActiveTermsInClass(classOf[LocalName])
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
            instance.localWitnessGuess.asScala.toMap ++ localWitnessGuessExtension.asScala

          val newlyCoveredAtomsOccurInChasedInstance = {
            val extendedGuess =
              extendedLocalWitnessGuess ++ instance.ruleConstantWitnessGuessAsMapToInstanceTerms.asScala

            val newlyCoveredAtoms =
              relevantSubquery.getAtoms.filter((atom: Atom) => {
                val atomVariables = atom.getVariables.toSet
                val allVariablesAreCovered = atomVariables.subsetOf(extendedGuess.keySet)

                // we no longer care about the part of the query
                // which entirely lies in the neighborhood of coexistential variables
                // of the instance
                val someVariableIsNewlyCovered =
                  atomVariables.exists(newlyCoveredVariables.contains)

                allVariablesAreCovered && someVariableIsNewlyCovered
              })

            newlyCoveredAtoms
              .map(atom =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess(_))
              )
              .forall(instance.localInstance.containsFact)
          }

          if (!newlyCoveredAtomsOccurInChasedInstance) boundary.break()

          val allSplitInstancesAreYesInstances = {
            val splitCoexistentialVariables =
              ConjunctiveQueryExtensions.connectedComponents(
                relevantSubquery,
                instance.coexistentialVariables.asScala.toSet -- newlyCoveredVariables.asScala
              )

            splitCoexistentialVariables.forall(splitCoexistentialVariablesComponent => {
              val newNeighbourhood =
                ConjunctiveQueryExtensions.neighbourhoodVariables(
                  relevantSubquery,
                  splitCoexistentialVariablesComponent
                ) -- instance.ruleConstantWitnessGuess.keySet.asScala

              // For the same reason as .get() call in the beginning of the method,
              // this .get() call succeeds.
              // noinspection OptionalGetWithoutIsPresent
              val newRelevantSubquery =
                ConjunctiveQueryExtensions
                  .subqueryRelevantToVariables(splitCoexistentialVariablesComponent)(
                    relevantSubquery
                  ).get

              val inducedInstance = new SubqueryEntailmentInstance(
                instance.ruleConstantWitnessGuess,
                ImmutableSet.copyOf(splitCoexistentialVariablesComponent.asJava),
                instance.localInstance,
                MapExtensions.restrictToKeys(
                  extendedLocalWitnessGuess.asJava,
                  newNeighbourhood.asJava
                ),
                MapExtensions.restrictToKeys(
                  instance.queryConstantEmbedding,
                  ConjunctiveQueryExtensions.constantsIn(newRelevantSubquery).asJava
                )
              )

              isYesInstance(inducedInstance)
            })
          }

          if (allSplitInstancesAreYesInstances) return true
      }

      // We tried all possible splits, and none of them worked.
      false
    }

    /**
     * Fill the DP table up to the given instance by chasing local instance when necessary.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = {
      if (this.table.containsKey(instance)) return
      val saturatedInstance =
        instance.withLocalInstance(datalogSaturationEngine.saturateInstance(
          saturatedRuleSet.saturatedRulesAsDatalogProgram,
          instance.localInstance,
          LocalInstanceTerm.RuleConstant(_)
        ))
      if (this.table.containsKey(saturatedInstance)) return

      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
        saturatedInstance.coexistentialVariables.asScala.toSet
      )(
        connectedConjunctiveQuery
      ).get

      // Check if the root instance can be split
      if (canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery, saturatedInstance)) {
        // we do not need to chase further
        this.table.put(saturatedInstance, true)
        return
      }
      // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
      // because they are treated as special symbols corresponding to variables and query constants
      // occurring in the subquery.
      val localNamesToPreserveDuringChase = SetLikeExtensions.union(
        saturatedInstance.localWitnessGuess.values,
        saturatedInstance.queryConstantEmbedding.values
      )
      // Chasing procedure:
      //   We hold a pair of (parent instance, children iterator) to the stack
      //   and perform a DFS. The children iterator is used to lazily visit
      //   the children of the parent instance. When we have found a YES instance,
      //   we mark all ancestors of the instance as YES. If we exhaust the children
      //   at any one node, we mark the node as NO and move back to the parent.
      //   Every time we chase, we add the chased local instance to localInstancesAlreadySeen
      //   to prevent ourselves from chasing the same instance twice.
      val stack =
        new java.util.ArrayDeque[Pair[
          SubqueryEntailmentInstance,
          java.util.Iterator[FormalInstance[LocalInstanceTerm]]
        ]]
      val localInstancesAlreadySeen = new java.util.HashSet[FormalInstance[LocalInstanceTerm]]

      // We begin DFS with the root saturated instance.
      stack.add(Pair.of(
        saturatedInstance,
        allNormalizedSaturatedChildrenOf(
          saturatedInstance.localInstance,
          localNamesToPreserveDuringChase
        ).iterator
      ))
      localInstancesAlreadySeen.add(saturatedInstance.localInstance)

      while (!stack.isEmpty) {
        import scala.util.boundary

        boundary:
          if (!stack.peekFirst.getRight.hasNext) {
            // if we have exhausted the children, we mark the current instance as NO
            val currentInstance = stack.pop.getLeft
            this.table.put(currentInstance, false)
            boundary.break()
          }

          // next instance that we wish to decide the entailment problem for
          val nextChasedInstance = stack.peekFirst.getRight.next // stack is non-empty here

          // if we have already seen this instance, we do not need to test for entailment
          // (we already know that it is a NO instance)
          if (localInstancesAlreadySeen.contains(nextChasedInstance)) boundary.break()
          else localInstancesAlreadySeen.add(nextChasedInstance)

          val chasedSubqueryEntailmentInstance =
            saturatedInstance.withLocalInstance(nextChasedInstance)

          val weAlreadyVisitedNextInstance =
            this.table.containsKey(chasedSubqueryEntailmentInstance)

          // if this is an instance we have already seen, we do not need to chase any further
          if (
            weAlreadyVisitedNextInstance && !this.table.get(chasedSubqueryEntailmentInstance)
          ) {
            // otherwise we proceed to the next sibling, so continue without modifying the stack
            boundary.break()
          }

          // If we know that the next instance is a YES instance,
          // or we can split the current instance into YES instances without chasing,
          // we mark all ancestors of the current instance as YES and exit
          if (
            (weAlreadyVisitedNextInstance && this.table.get(
              chasedSubqueryEntailmentInstance
            )) || canBeSplitIntoYesInstancesWithoutChasing(
              relevantSubquery,
              chasedSubqueryEntailmentInstance
            )
          ) {
            for (ancestor <- stack.asScala) { this.table.put(ancestor.getLeft, true) }
            // we also put the original (unsaturated) instance into the table
            this.table.put(instance, true)
            return
          }
          // It turned out that we cannot split the current instance into YES instances.
          // At this point, we need to chase further down the shortcut chase tree.
          // So we push the chased instance onto the stack and keep on.
          stack.push(Pair.of(
            chasedSubqueryEntailmentInstance,
            allNormalizedSaturatedChildrenOf(
              nextChasedInstance,
              localNamesToPreserveDuringChase
            ).iterator
          ))
      }
      // At this point, we have marked the saturated root instance as NO (since we emptied the stack
      // and every time we pop an instance from the stack, we marked that instance as NO).
      // Finally, we mark the original (unsaturated) instance as NO as well.
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
      saturatedRuleSet.allRules.asScala.toSet,
      connectedConjunctiveQuery
    ).maxArity
    val dpTable = new DPTable(
      saturatedRuleSet,
      extensionalSignature,
      maxArityOfAllPredicatesUsedInRules,
      connectedConjunctiveQuery
    )

    DFSNormalizingDPTableSEEnumeration.allWellFormedNormalizedSubqueryEntailmentInstancesFor(
      extensionalSignature,
      ruleConstants,
      connectedConjunctiveQuery
    ).forEach(dpTable.fillTableUpto(_))
    dpTable.getKnownYesInstances
  }

  override def toString: String =
    "DFSNormalizingDPTableSEEnumeration{datalogSaturationEngine=" + datalogSaturationEngine + '}'
}
