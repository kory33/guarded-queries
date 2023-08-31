package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.RuleConstant
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces.*
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.ListExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.*
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import scala.collection.mutable
import scala.util.boundary

/**
 * An implementation of subquery entailment enumeration using a DP table together with efficient
 * DFS traversal and normalization.
 */
object DFSNormalizingDPTableSEEnumeration {

  /**
   * Checks whether the given set of local names is of a form {0, ..., n - 1} for some n.
   */
  private def isZeroStartingContiguousLocalNameSet(localNames: Set[LocalName]) = {
    var firstElementAfterZeroNotContainedInSet = 0
    while (localNames.contains(LocalName(firstElementAfterZeroNotContainedInSet)))
      firstElementAfterZeroNotContainedInSet += 1
    firstElementAfterZeroNotContainedInSet == localNames.size
  }

  private def allNormalizedLocalInstances(extensionalSignature: FunctionFreeSignature,
                                          ruleConstants: Set[Constant]
  ) = {
    val maxArityOfExtensionalSignature = extensionalSignature.maxArity
    val ruleConstantsAsLocalTerms = ruleConstants.map(LocalInstanceTerm.RuleConstant.apply)

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
    val localNames = (0 until maxArityOfExtensionalSignature)
      .map(LocalInstanceTerm.LocalName.apply)
      .toSet

    val allLocalInstanceTerms = localNames ++ ruleConstantsAsLocalTerms
    val predicates = extensionalSignature.predicates

    val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
      val allFormalFactsOverThePredicate =
        val parameterIndices = 0 until predicate.getArity

        allTotalFunctionsBetween(parameterIndices.toSet, allLocalInstanceTerms)
          .map(parameterMap =>
            FormalFact(predicate, parameterIndices.map(parameterMap.apply).toList)
          ).toSet

      allFormalFactsOverThePredicate.powerset.map(FormalInstance.apply)
    }

    val allInstancesOverLocalNameSet = predicates.toList
      .productMappedIterablesToLists(allLocalInstancesOverThePredicate)
      .map(FormalInstance.unionAll)

    allInstancesOverLocalNameSet.filter(instance =>
      isZeroStartingContiguousLocalNameSet(instance.getActiveTermsIn[LocalName])
    )
  }

  private def allWellFormedNormalizedSubqueryEntailmentInstancesFor(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: Set[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ) = {
    val queryVariables = conjunctiveQuery.allVariables
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: Map[Variable, Constant]) => {
        val allCoexistentialVariableSets =
          queryExistentialVariables
            .powerset
            .filter(_.nonEmpty)
            .filter(!_.intersects(ruleConstantWitnessGuess.keySet))
            .filter(variableSet => conjunctiveQuery.connects(variableSet))

        allCoexistentialVariableSets.flatMap((coexistentialVariables: Set[Variable]) =>
          allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(
            localInstance => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = conjunctiveQuery
                .subqueryRelevantToVariables(coexistentialVariables).get

              val nonConstantNeighbourhood = conjunctiveQuery
                .strictNeighbourhoodOf(
                  coexistentialVariables
                ) -- ruleConstantWitnessGuess.keySet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance.getActiveTermsIn[LocalName].filter(localName =>
                  localName.value < nonConstantNeighbourhood.size
                )
              )

              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants = relevantSubquery.allConstants -- ruleConstants

                val nonWitnessingActiveLocalNames =
                  localInstance.getActiveTermsIn[LocalName] --
                    localWitnessGuess.values

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

final class DFSNormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {

  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {
    final private val table = mutable.HashMap[SubqueryEntailmentInstance, Boolean]()
    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      fillTableUpto(instance)
      this.table(instance)
    }

    /**
     * Returns a stream of all children (in the shortcut chase tree) of the given saturated
     * instance
     */
    private def allNormalizedSaturatedChildrenOf(
      saturatedInstance: LocalInstance,
      namesToBePreserved: Set[LocalName]
    ): Set[LocalInstance] = {
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
        def foo(existentialRule: NormalGTGD): List[LocalInstance] = {
          val headAtom = existentialRule.getHeadAtoms()(0)
          // A set of existential variables in the existential rule
          val existentialVariables = existentialRule.getHead.getBoundVariables.toSet
          val bodyJoinResult =
            FilterNestedLoopJoin[LocalInstanceTerm].join(
              existentialRule.bodyAsCQ,
              saturatedInstance
            )

          // because we are "reusing" local names, we can no longer
          // uniformly extend homomorphisms to existential variables
          // (i.e. local names to which existential variables are mapped depend on
          //  how frontier variables are mapped to local names, as those are the
          //  local names that get inherited to the child instance)
          bodyJoinResult.allHomomorphisms.flatMap(bodyHomomorphism => {
            def foo(bodyHomomorphism: HomomorphicMapping[LocalInstanceTerm])
              : IterableOnce[LocalInstance] = {
              // The set of local names that are inherited from the parent instance
              // to the child instance.
              val inheritedLocalNames =
                existentialRule.frontierVariables.map(bodyHomomorphism.apply)

              // Names we can reuse (i.e. assign to existential variables in the rule)
              // in the child instance. All names in this set should be considered distinct
              // from the names in the parent instance having the same value, so we
              // are explicitly ignoring the "implicit equality coding" semantics here.
              val namesToReuseInChild =
                (0 until maxArityOfAllPredicatesUsedInRules).map(LocalName.apply)
                  .toSet.removedAll(inheritedLocalNames)
                  .toList

              val headVariableHomomorphism =
                existentialVariables
                  .zipWithIndex
                  .map { (variable, index) => (variable, namesToReuseInChild(index)) }
                  .toMap

              val extendedHomomorphism =
                bodyHomomorphism.extendWithMapping(headVariableHomomorphism)
                // The instance containing only the head atom produced by the existential rule.
                // This should be a singleton instance because the existential rule is normal.
              val headInstance =
                FormalInstance.of(extendedHomomorphism.materializeFunctionFreeAtom(headAtom))

              // if names are not preserved, we reject this homomorphism
              if (!namesToBePreserved.widen[LocalInstanceTerm].subsetOf(inheritedLocalNames))
                return Set.empty

              // The set of facts in the parent instance that are
              // "guarded" by the head of the existential rule.
              // Those are precisely the facts that have its local names
              // appearing in the head of the existential rule
              // as a homomorphic image of a frontier variable in the rule.
              val inheritedFactsInstance = saturatedInstance.restrictToAlphabetsWith(term =>
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
                  headInstance
                )

              // we only need to keep chasing with extensional signature
              Set(childInstance.restrictToSignature(extensionalSignature))
            }
            foo(bodyHomomorphism)
          })
        }
        foo(existentialRule)
      }
      saturatedRuleSet.existentialRules.flatMap(allChasesWithRule.apply)
    }

    /**
     * Check if the given instance (whose relevant subquery is given) can be split into YES
     * instances without chasing further.
     *
     * @param relevantSubquery
     *   this must equal
     *   `connectedConjunctiveQuery.subqueryRelevantToVariables(instance.coexistentialVariables).get()`
     */
    private def canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery: ConjunctiveQuery,
                                                         instance: SubqueryEntailmentInstance
    ): Boolean = boundary { returnMethod ?=>
      val localWitnessGuessExtensions = allPartialFunctionsBetween(
        instance.coexistentialVariables,
        instance.localInstance.getActiveTermsIn[LocalName]
      )

      for (localWitnessGuessExtension <- localWitnessGuessExtensions) {
        boundary: continueInnerLoop ?=>
          if (localWitnessGuessExtension.isEmpty) {
            // we do not allow "empty split"; whenever we split (i.e. make some progress
            // in the chase automaton), we must pick a nonempty set of coexistential variables
            // to map to local names in the chased instance.
            boundary.break()(using continueInnerLoop)
          }

          val newlyCoveredVariables = localWitnessGuessExtension.keySet
          val extendedLocalWitnessGuess =
            instance.localWitnessGuess.toMap ++ localWitnessGuessExtension

          val newlyCoveredAtomsOccurInChasedInstance = {
            val extendedGuess =
              extendedLocalWitnessGuess ++ instance.ruleConstantWitnessGuessAsMapToInstanceTerms

            val newlyCoveredAtoms =
              relevantSubquery.getAtoms.filter((atom: Atom) => {
                val atomVariables = atom.getVariables.toSet
                val allVariablesAreCovered = atomVariables.subsetOf(extendedGuess.keySet)

                // we no longer care about the part of the query
                // which entirely lies in the neighborhood of coexistential variables
                // of the instance
                val someVariableIsNewlyCovered =
                  atomVariables.intersects(newlyCoveredVariables)

                allVariablesAreCovered && someVariableIsNewlyCovered
              })

            newlyCoveredAtoms
              .map(atom =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess.apply)
              )
              .forall(instance.localInstance.containsFact)
          }

          if (!newlyCoveredAtomsOccurInChasedInstance) boundary.break()(using continueInnerLoop)

          val allSplitInstancesAreYesInstances = {
            val splitCoexistentialVariables =
              relevantSubquery.connectedComponentsOf(
                instance.coexistentialVariables -- newlyCoveredVariables
              )

            splitCoexistentialVariables.forall(splitCoexistentialVariablesComponent => {
              val newNeighbourhood =
                relevantSubquery.strictNeighbourhoodOf(
                  splitCoexistentialVariablesComponent
                ) -- instance.ruleConstantWitnessGuess.keySet

              // For the same reason as .get() call in the beginning of the method,
              // this .get() call succeeds.
              // noinspection OptionalGetWithoutIsPresent
              val newRelevantSubquery =
                relevantSubquery
                  .subqueryRelevantToVariables(splitCoexistentialVariablesComponent)
                  .get

              val inducedInstance = SubqueryEntailmentInstance(
                instance.ruleConstantWitnessGuess,
                splitCoexistentialVariablesComponent,
                instance.localInstance,
                extendedLocalWitnessGuess.restrictToKeys(newNeighbourhood),
                instance.queryConstantEmbedding.restrictToKeys(newRelevantSubquery.allConstants)
              )

              isYesInstance(inducedInstance)
            })
          }

          if (allSplitInstancesAreYesInstances) boundary.break(true)(using returnMethod)
      }

      // We tried all possible splits, and none of them worked.
      false
    }

    /**
     * Fill the DP table up to the given instance by chasing local instance when necessary.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = {
      if (this.table.contains(instance)) return
      val saturatedInstance =
        instance.withLocalInstance(datalogSaturationEngine.saturateInstance(
          saturatedRuleSet.saturatedRulesAsDatalogProgram,
          instance.localInstance
        ))
      if (this.table.contains(saturatedInstance)) return

      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = connectedConjunctiveQuery
        .subqueryRelevantToVariables(saturatedInstance.coexistentialVariables)
        .get

      // Check if the root instance can be split
      if (canBeSplitIntoYesInstancesWithoutChasing(relevantSubquery, saturatedInstance)) {
        // we do not need to chase further
        this.table.put(saturatedInstance, true)
        return
      }
      // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
      // because they are treated as special symbols corresponding to variables and query constants
      // occurring in the subquery.
      val localNamesToPreserveDuringChase =
        saturatedInstance.localWitnessGuess.values.toSet ++ saturatedInstance.queryConstantEmbedding.values

      // Chasing procedure:
      //   We hold a pair of (parent instance, children iterator) to the stack
      //   and perform a DFS. The children iterator is used to lazily visit
      //   the children of the parent instance. When we have found a YES instance,
      //   we mark all ancestors of the instance as YES. If we exhaust the children
      //   at any one node, we mark the node as NO and move back to the parent.
      //   Every time we chase, we add the chased local instance to localInstancesAlreadySeen
      //   to prevent ourselves from chasing the same instance twice.
      var stack = List[(SubqueryEntailmentInstance, Iterator[LocalInstance])]()
      val localInstancesAlreadySeen = mutable.HashSet[LocalInstance]()

      // We begin DFS with the root saturated instance.
      stack ::= ((
        saturatedInstance,
        allNormalizedSaturatedChildrenOf(
          saturatedInstance.localInstance,
          localNamesToPreserveDuringChase
        ).iterator
      ))
      localInstancesAlreadySeen.add(saturatedInstance.localInstance)

      while (stack.nonEmpty) {
        import scala.util.boundary

        boundary:
          val (nextInstance, nextChildrenIterator) = stack.head
          if (!nextChildrenIterator.hasNext) {
            // if we have exhausted the children, we mark the current instance as NO
            stack = stack.tail
            this.table.put(nextInstance, false)
            boundary.break()
          }

          // next instance that we wish to decide the entailment problem for
          val nextChasedInstance = nextChildrenIterator.next

          // if we have already seen this instance, we do not need to test for entailment
          // (we already know that it is a NO instance)
          if (localInstancesAlreadySeen.contains(nextChasedInstance)) boundary.break()
          else localInstancesAlreadySeen.add(nextChasedInstance)

          val chasedSubqueryEntailmentInstance =
            saturatedInstance.withLocalInstance(nextChasedInstance)

          val weAlreadyVisitedNextInstance =
            this.table.contains(chasedSubqueryEntailmentInstance)

          // if this is an instance we have already seen, we do not need to chase any further
          if (weAlreadyVisitedNextInstance && !this.table(chasedSubqueryEntailmentInstance)) {
            // otherwise we proceed to the next sibling, so continue without modifying the stack
            boundary.break()
          }

          // If we know that the next instance is a YES instance,
          // or we can split the current instance into YES instances without chasing,
          // we mark all ancestors of the current instance as YES and exit
          if (
            (weAlreadyVisitedNextInstance && this.table(
              chasedSubqueryEntailmentInstance
            )) || canBeSplitIntoYesInstancesWithoutChasing(
              relevantSubquery,
              chasedSubqueryEntailmentInstance
            )
          ) {
            stack.foreach((ancestor, _) => this.table.put(ancestor, true))
            // we also put the original (unsaturated) instance into the table
            this.table.put(instance, true)
            return
          }

          // It turned out that we cannot split the current instance into YES instances.
          // At this point, we need to chase further down the shortcut chase tree.
          // So we push the chased instance onto the stack and keep on.
          stack ::= ((
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

    def getKnownYesInstances: IterableOnce[SubqueryEntailmentInstance] =
      this.table.filter(_._2).keys
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): IterableOnce[SubqueryEntailmentInstance] = {
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

    DFSNormalizingDPTableSEEnumeration.allWellFormedNormalizedSubqueryEntailmentInstancesFor(
      extensionalSignature,
      ruleConstants,
      connectedConjunctiveQuery
    ).foreach(dpTable.fillTableUpto.apply)
    dpTable.getKnownYesInstances
  }

  override def toString: String =
    "DFSNormalizingDPTableSEEnumeration{datalogSaturationEngine=" + datalogSaturationEngine + '}'
}
