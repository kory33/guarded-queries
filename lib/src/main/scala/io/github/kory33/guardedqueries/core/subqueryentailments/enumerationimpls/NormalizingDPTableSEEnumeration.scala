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
 * An implementation of subquery entailment enumeration using a DP table plus a simple
 * normalization.
 */
final class NormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {
    final private val table = mutable.HashMap[SubqueryEntailmentInstance, Boolean]()

    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      if (!this.table.contains(instance)) fillTableUpto(instance)
      this.table(instance)
    }

    private def chaseLocalInstance(localInstance: LocalInstance,
                                   namesToBePreservedDuringChase: Set[LocalName]
    ): Set[LocalInstance] = {
      val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram
      val shortcutChaseOneStep = (instance: LocalInstance) => {
        def foo(instance: LocalInstance): Set[LocalInstance] = {
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

              val bodyJoinResult = FilterNestedLoopJoin[LocalInstanceTerm]
                .join(existentialRule.bodyAsCQ, instance)

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
                      .toVector

                  val headVariableHomomorphism =
                    existentialVariables
                      .zipWithIndex
                      .map { (variable, index) => (variable, namesToReuseInChild(index)) }
                      .toMap

                  val extendedHomomorphism =
                    bodyHomomorphism.extendWithMap(headVariableHomomorphism)

                  // The instance containing only the head atom produced by the existential rule.
                  // This should be a singleton instance because the existential rule is normal.
                  val headInstance = FormalInstance.of(
                    extendedHomomorphism.materializeFunctionFreeAtom(headAtom)
                  )

                  // if names are not preserved, we reject this homomorphism
                  if (
                    !namesToBePreservedDuringChase
                      .widen[LocalInstanceTerm]
                      .subsetOf(inheritedLocalNames)
                  )
                    return Set.empty

                  // The set of facts in the parent instance that are
                  // "guarded" by the head of the existential rule.
                  // Those are precisely the facts that have its local names
                  // appearing in the head of the existential rule
                  // as a homomorphic image of a frontier variable in the rule.
                  val inheritedFactsInstance = instance.restrictToAlphabetsWith(term =>
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

          val children = saturatedRuleSet.existentialRules.flatMap(allChasesWithRule.apply)
          children
        }

        foo(instance)
      }

      // we keep chasing until we reach a fixpoint
      Set(datalogSaturationEngine.saturateInstance(
        datalogSaturation,
        localInstance
      )).generateFromElementsUntilFixpoint(shortcutChaseOneStep.apply)
    }

    /**
     * Fill the DP table up to the given instance.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = boundary { returnMethod ?=>
      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = connectedConjunctiveQuery.subqueryRelevantToVariables(
        instance.coexistentialVariables
      ).get

      val instancesWithGuessedVariablesPreserved = chaseLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        instance.localWitnessGuess.values.toSet ++ instance.queryConstantEmbedding.asMap.values
      )

      for (chasedInstance <- instancesWithGuessedVariablesPreserved) {
        val localWitnessGuessExtensions = allPartialFunctionsBetween(
          instance.coexistentialVariables,
          chasedInstance.getActiveTermsIn[LocalName]
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
              val coveredVariables = extendedGuess.keySet
              val newlyCoveredAtoms = relevantSubquery.getAtoms.filter((atom: Atom) => {
                val atomVariables = atom.getVariables.toSet
                val allVariablesAreCovered = atomVariables.subsetOf(coveredVariables)

                // we no longer care about the part of the query
                // which entirely lies in the neighborhood of coexistential variables
                // of the instance
                val someVariableIsNewlyCovered =
                  atomVariables.intersects(newlyCoveredVariables)

                allVariablesAreCovered && someVariableIsNewlyCovered
              })

              newlyCoveredAtoms.map((atom: Atom) =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess.apply)
              ).forall(chasedInstance.containsFact)
            }

            if (!newlyCoveredAtomsOccurInChasedInstance)
              boundary.break()(using continueInnerLoop)

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
                  chasedInstance,
                  extendedLocalWitnessGuess.restrictToKeys(newNeighbourhood),
                  instance.queryConstantEmbedding.restrictToKeys(
                    newRelevantSubquery.allConstants
                  )
                )

                isYesInstance(inducedInstance)
              })
            }

            if (allSplitInstancesAreYesInstances) {
              this.table.put(instance, true)
              boundary.break()(using returnMethod)
            }
        }
      }

      // all instances chased from the original instance fail to fulfill the subquery
      // strongly induced by instance.coexistentialVariables(), so we mark the original instance false.
      this.table.put(instance, false)
    }

    def getKnownYesInstances: Iterable[SubqueryEntailmentInstance] =
      this.table.filter(_._2).keys
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): Iterable[SubqueryEntailmentInstance] = {
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
    ).foreach(dpTable.fillTableUpto.apply)

    dpTable.getKnownYesInstances
  }
  override def toString: String =
    "NormalizingDPTableSEEnumeration{" + "datalogSaturationEngine=" + datalogSaturationEngine + '}'
}

object NormalizingDPTableSEEnumeration {

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
      val predicateParameterIndices = (0 until predicate.getArity).toSet

      val allFormalFactsOverThePredicate = allTotalFunctionsBetween(
        predicateParameterIndices,
        allLocalInstanceTerms
      ).map(parameterMap => {
        FormalFact(
          predicate,
          (0 until predicate.getArity).map(parameterMap.apply).toList
        )
      })

      allFormalFactsOverThePredicate.toSet.powerset.map(FormalInstance.apply)
    }

    val allInstancesOverLocalNameSet = predicates.toList
      .productAll(allLocalInstancesOverThePredicate)
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

        val allCoexistentialVariableSets = queryExistentialVariables.powerset
          .filter(_.nonEmpty)
          .filter(_ disjointFrom ruleConstantWitnessGuess.keySet)
          .filter(variableSet => conjunctiveQuery.connects(variableSet))

        allCoexistentialVariableSets.flatMap((coexistentialVariables: Set[Variable]) =>
          allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(
            (localInstance: LocalInstance) => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = conjunctiveQuery
                .subqueryRelevantToVariables(coexistentialVariables)
                .get

              val nonConstantNeighbourhood =
                conjunctiveQuery.strictNeighbourhoodOf(
                  coexistentialVariables
                ) -- ruleConstantWitnessGuess.keySet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance
                  .getActiveTermsIn[LocalName]
                  .filter(_.value < nonConstantNeighbourhood.size)
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
                  SubqueryEntailmentInstance(
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
