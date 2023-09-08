package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.*
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.{LocalName, RuleConstant}
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces.*
import io.github.kory33.guardedqueries.core.utils.extensions.*
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Variable}

import scala.collection.mutable

/**
 * An implementation of subquery entailment enumeration using a DP table together with efficient
 * DFS traversal and normalization.
 */
final class DFSNormalizingDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  private def isSubqueryEntailmentCached(
    extensionalSignature: FunctionFreeSignature,
    saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
    connectedConjunctiveQuery: ConjunctiveQuery
  ): SubqueryEntailmentInstance => Boolean = {
    val maxArityOfAllPredicatesUsedInRules =
      FunctionFreeSignature
        .encompassingRuleQuery(saturatedRuleSet.allRules, connectedConjunctiveQuery)
        .maxArity

    val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram

    def saturatedNormalizedChildrenOf(saturatedInstance: LocalInstance,
                                      namesToBePreservedDuringChase: Set[LocalName]
    ): Iterable[LocalInstance] = {
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
          .joinConjunctiveQuery(existentialRule.bodyAsCQ, saturatedInstance)
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
              val inheritedFactsInstance = saturatedInstance.restrictToAlphabetsWith(term =>
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

    // The cache acting as a DP table.
    val subqueryEntailmentMemoization =
      mutable.HashMap.empty[SubqueryEntailmentInstance, Boolean]

    enum SubqueryEntailmentRecursionResult:
      case Entailed
      case NotEntailed
      case AlreadyVisitedAndUnknown
    import SubqueryEntailmentRecursionResult.*

    extension (b: Boolean)
      private def asSubqueryEntailmentRecursionResult: SubqueryEntailmentRecursionResult =
        if b then SubqueryEntailmentRecursionResult.Entailed
        else SubqueryEntailmentRecursionResult.NotEntailed

    // We now define the three-way mutual recursion among isSubqueryEntailment, splitsAtInstance and splitsAtInstanceWith.
    def isSubqueryEntailment(inputInstance: SubqueryEntailmentInstance): Boolean = {
      def performDFSOnChase(
        instance: SubqueryEntailmentInstance,
        isLocalInstanceSaturatedAPriori: Boolean,
        localInstancesSeenInThisRecursion: mutable.Set[LocalInstance]
      ): SubqueryEntailmentRecursionResult = {
        if (subqueryEntailmentMemoization.contains(instance)) {
          subqueryEntailmentMemoization(instance).asSubqueryEntailmentRecursionResult
        } else if (!localInstancesSeenInThisRecursion.contains(instance.localInstance)) {
          val result: Boolean = {
            if (!isLocalInstanceSaturatedAPriori) {
              val saturatedInstance = instance.withLocalInstance {
                datalogSaturationEngine.saturateInstance(
                  datalogSaturation,
                  instance.localInstance
                )
              }

              performDFSOnChase(
                saturatedInstance,
                true,
                localInstancesSeenInThisRecursion
              ) == Entailed
            } else {
              // The subquery for which we are trying to decide the entailment problem.
              given relevantSubquery: ConjunctiveQuery = {
                // If the instance is well-formed, the variable set is non-empty and connected,
                // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
                connectedConjunctiveQuery
                  .subqueryRelevantToVariables(instance.coexistentialVariables)
                  .get
              }

              localInstancesSeenInThisRecursion.add(instance.localInstance)

              // `instance` is a subquery entailment if and only if `instance` could be a committing point
              // or one of its children is a subquery entailment. By making a recursive call to isSubqueryEntailment
              // with a child instance, we are essentially performing a DFS traversal of the shortcut chase tree.
              splitsAtInstance(instance) || {
                saturatedNormalizedChildrenOf(
                  instance.localInstance,
                  // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
                  // because they are treated as special symbols corresponding to variables and query constants
                  // occurring in the subquery.
                  instance.localWitnessGuess.values.toSet ++ instance.queryConstantEmbedding.values
                ).exists(saturatedNormalizedChild =>
                  performDFSOnChase(
                    instance.withLocalInstance(saturatedNormalizedChild),
                    true,
                    localInstancesSeenInThisRecursion
                  ) == Entailed
                )
              }
            }
          }

          subqueryEntailmentMemoization.put(instance, result)
          result.asSubqueryEntailmentRecursionResult
        } else {
          // We have already seen this local instance in this recursion,
          // so we need to answer "we don't know" to avoid infinite recursion.
          AlreadyVisitedAndUnknown
        }
      }

      performDFSOnChase(inputInstance, false, mutable.HashSet.empty) == Entailed
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
        splitInstances.newlyCommittedPart.forall(entailmentInstance.localInstance.containsFact)

      def allComponentsSatisfied =
        splitInstances.subInstances.forall(isSubqueryEntailment)

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
          saturatedRuleSet.constants.map(RuleConstant.apply),
          connectedConjunctiveQuery
        )
      if isSubqueryEntailment(subqueryEntailmentInstance)
    } yield subqueryEntailmentInstance
  }

  override def toString: String =
    s"DFSNormalizingDPTableSEEnumeration{datalogSaturationEngine=$datalogSaturationEngine}"
}
