package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.{
  LocalName,
  RuleConstant
}
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm,
  SubqueryEntailmentEnumeration,
  SubqueryEntailmentInstance
}
import io.github.kory33.guardedqueries.core.utils.CachingFunction
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces.*
import io.github.kory33.guardedqueries.core.utils.extensions.*
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Constant, Predicate, Variable}

/**
 * An implementation of subquery entailment enumeration using a DP table plus a simple
 * normalization.
 */
final class NormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  private def isSubqueryEntailmentCached(
    extensionalSignature: FunctionFreeSignature,
    saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
    connectedConjunctiveQuery: ConjunctiveQuery
  ): CachingFunction[SubqueryEntailmentInstance, Boolean] = {
    val maxArityOfAllPredicatesUsedInRules =
      FunctionFreeSignature
        .encompassingRuleQuery(saturatedRuleSet.allRules, connectedConjunctiveQuery)
        .maxArity

    def chaseNormalizedLocalInstance(localInstance: LocalInstance,
                                     namesToBePreservedDuringChase: Set[LocalName]
    ): Set[LocalInstance] = {
      val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram
      val shortcutChaseOneStep = (instance: LocalInstance) => {
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
        val allNormalizedChildrenWithRule = (existentialRule: NormalGTGD) => {
          // A set of existential variables in the existential rule
          val existentialVariables = existentialRule.getHead.getBoundVariables.toSet

          FilterNestedLoopJoin[Variable, LocalInstanceTerm]
            .joinConjunctiveQuery(existentialRule.bodyAsCQ, instance)
            .allHomomorphisms
            .associate { bodyHomomorphism =>
              // The set of local names that are inherited from the parent instance
              // to the child instance.
              existentialRule.frontierVariables.map(bodyHomomorphism)
            }
            .filter { (_, inheritedLocalNames) =>
              // we accept a homomorphism only if names are preserved
              namesToBePreservedDuringChase subsetOfSupertypeSet inheritedLocalNames
            }
            .map { (bodyHomomorphism, inheritedLocalNames) =>
              // The child instance, which is the saturation of the union of
              // the set of inherited facts and the head instance.
              val childInstance = {
                // The set of facts in the parent instance that are
                // "guarded" by the head of the existential rule.
                // Those are precisely the facts that have its local names
                // appearing in the head of the existential rule
                // as a homomorphic image of a frontier variable in the rule.
                val inheritedFactsInstance = instance.restrictToAlphabetsWith(term =>
                  term.isConstantOrSatisfies(inheritedLocalNames.contains)
                )

                val materializedHead = {
                  // Names we can reuse (i.e. assign to existential variables in the rule)
                  // in the child instance. All names in this set should be considered distinct
                  // from the names in the parent instance having the same value, so we
                  // are explicitly ignoring the "implicit equality coding" semantics here.
                  val namesToReuseInChild = (0 until maxArityOfAllPredicatesUsedInRules)
                    .map(LocalName(_))
                    .toSet.removedAll(inheritedLocalNames)
                    .toVector

                  bodyHomomorphism
                    .extendWithMap(existentialVariables.zip(namesToReuseInChild).toMap)
                    .materializeFunctionFreeAtom(existentialRule.getHeadAtoms.head)
                }

                datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                  datalogSaturation,
                  // because the parent is saturated, a restriction of it to the alphabet
                  // occurring in the child is also saturated.
                  inheritedFactsInstance,
                  FormalInstance.of(materializedHead)
                )
              }

              // we only need to keep chasing with extensional signature
              childInstance.restrictToSignature(extensionalSignature)
            }
        }

        saturatedRuleSet.existentialRules.flatMap(allNormalizedChildrenWithRule)
      }

      // we keep chasing from the saturated input instance until we reach a fixpoint
      val saturatedInputInstance = datalogSaturationEngine
        .saturateInstance(datalogSaturation, localInstance)
      Set(saturatedInputInstance).generateFromElementsUntilFixpoint(shortcutChaseOneStep)
    }

    // The rest of this function is exactly the same as that of NaiveDPTableSEEnumeration.

    // We now define the three-way mutual recursion among isSubqueryEntailment, splitsAtInstance and splitsAtInstanceWith.
    // We use lazy val to allow a forward reference of splitsAtInstance from isSubqueryEntailment.
    lazy val isSubqueryEntailment = CachingFunction { (instance: SubqueryEntailmentInstance) =>
      // The subquery for which we are trying to decide the entailment problem.
      given relevantSubquery: ConjunctiveQuery = {
        // If the instance is well-formed, the variable set is non-empty and connected,
        // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
        connectedConjunctiveQuery
          .subqueryRelevantToVariables(instance.coexistentialVariables)
          .get
      }

      chaseNormalizedLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        instance.localWitnessGuess.values.toSet ++ instance.queryConstantEmbedding.values
      ).exists(chasedInstance => splitsAtInstance(instance.withLocalInstance(chasedInstance)))
    }

    def splitsAtInstance(
      instance: SubqueryEntailmentInstance
    )(using relevantSubquery: ConjunctiveQuery): Boolean = {
      allPartialFunctionsBetween(
        instance.coexistentialVariables,
        instance.localInstance.activeLocalNames
      )
        .filter(_.nonEmpty) // we require commit maps to be nonempty
        .exists(splitsAtInstanceWith(instance, _))
    }

    def splitsAtInstanceWith(
      entailmentInstance: SubqueryEntailmentInstance,
      commitMap: Map[Variable, LocalName]
    )(using relevantSubquery: ConjunctiveQuery): Boolean = {
      val splitInstances = entailmentInstance.splitIntoSubInstances(commitMap)

      def baseSatisfied =
        splitInstances.newlyCommittedPart
          .forall(entailmentInstance.localInstance.containsFact)

      def allComponentsSatisfied =
        splitInstances.subInstances
          .forall(isSubqueryEntailment)

      baseSatisfied && allComponentsSatisfied
    }

    isSubqueryEntailment
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): Iterable[SubqueryEntailmentInstance] = {
    val isSubqueryEntailment = isSubqueryEntailmentCached(
      extensionalSignature,
      saturatedRuleSet,
      connectedConjunctiveQuery
    )

    for {
      subqueryEntailmentInstance <-
        NormalizingDPTableSEEnumeration.allWellFormedNormalizedSubqueryEntailmentInstances(
          extensionalSignature,
          saturatedRuleSet.constants,
          connectedConjunctiveQuery
        )
      if isSubqueryEntailment(subqueryEntailmentInstance)
    } yield subqueryEntailmentInstance
  }

  override def toString: String =
    s"NormalizingDPTableSEEnumeration{datalogSaturationEngine=$datalogSaturationEngine}"
}

object NormalizingDPTableSEEnumeration {

  /**
   * Checks whether the given set of local names is of a form {0, ..., n - 1} for some n.
   */
  private def isZeroStartingContiguousLocalNameSet(localNames: Set[LocalName]) =
    (0 until localNames.size)
      .forall { name => localNames.contains(LocalName(name)) }

  private def allNormalizedLocalInstances(extensionalSignature: FunctionFreeSignature,
                                          ruleConstants: Set[Constant]
  ) = {
    // We need to consider sufficiently large collection of set of active local names.
    // As it is sufficient to check subquery entailments for all guarded instance
    // over the extensional signature, and the extensional signature has
    // maxArityOfExtensionalSignature as the maximal arity, we only need to
    // consider a powerset of {0, ..., maxArityOfExtensionalSignature - 1}
    // (this is by normalization: we can remap all local names in the range
    //  [maxArityOfExtensionalSignature, maxArityOfExtensionalSignature * 2)
    //  to the range [0, maxArityOfExtensionalSignature), since the size of
    //  active local name set of local instances necessary to check
    //  is at most maxArityOfExtensionalSignature.
    //  Moreover, by symmetry of instance we can demand that the set of active
    //  names to be contiguous and starting from 0, i.e. {0, ..., n} for some n < maxArityOfExtensionalSignature.
    // )
    val allActiveLocalNameSets =
      (0 until extensionalSignature.maxArity).map(LocalName(_): LocalName)
        .toSet
        .powerset
        .filter(isZeroStartingContiguousLocalNameSet)

    allActiveLocalNameSets.flatMap(localNames => {
      val allLocalInstanceTerms = localNames ++ ruleConstants.map(RuleConstant(_))

      val allFormalFactsOverPredicate = (predicate: Predicate) =>
        allLocalInstanceTerms
          .naturalPowerTo(predicate.getArity)
          .map(FormalFact(predicate, _))

      val allInstancesOverLocalNameSet =
        extensionalSignature.predicates
          .flatMap(allFormalFactsOverPredicate)
          .powerset.map(FormalInstance(_))

      // To avoid generating duplicate instances, we only take
      // instances that use all local names in localNames.
      allInstancesOverLocalNameSet.filter(_.activeLocalNames.size == localNames.size)
    })
  }

  def allWellFormedNormalizedSubqueryEntailmentInstances(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: Set[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ): Iterable[SubqueryEntailmentInstance] = {
    val queryVariables = conjunctiveQuery.allVariables
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    for {
      ruleConstantWitnessGuess <- allPartialFunctionsBetween(queryVariables, ruleConstants)
      variablesGuessedToBeRuleConstants = ruleConstantWitnessGuess.keySet

      coexistentialVariables <- queryExistentialVariables.powerset
        .filter(_.nonEmpty)
        .filter(_ disjointFrom variablesGuessedToBeRuleConstants)
        .filter(variableSet => conjunctiveQuery.connects(variableSet))

      relevantSubquery =
        // As coexistentialVariables is a nonempty subset of queryVariables,
        // we expect to see a non-empty optional.
        conjunctiveQuery.subqueryRelevantToVariables(coexistentialVariables).get
      nonConstantNeighbourhood =
        conjunctiveQuery
          .strictNeighbourhoodOf(coexistentialVariables) -- variablesGuessedToBeRuleConstants

      localInstance <- allNormalizedLocalInstances(extensionalSignature, ruleConstants)
      localWitnessGuess <-
        allFunctionsBetween(nonConstantNeighbourhood, localInstance.activeLocalNames)
      queryConstantEmbedding <- {
        val subqueryConstants = relevantSubquery.allConstants -- ruleConstants
        val nonWitnessingActiveLocalNames =
          localInstance.activeLocalNames -- localWitnessGuess.values

        allInjectionsBetween(subqueryConstants, nonWitnessingActiveLocalNames)
      }
    } yield SubqueryEntailmentInstance(
      ruleConstantWitnessGuess,
      coexistentialVariables,
      localInstance,
      localWitnessGuess,
      queryConstantEmbedding
    )
  }
}
