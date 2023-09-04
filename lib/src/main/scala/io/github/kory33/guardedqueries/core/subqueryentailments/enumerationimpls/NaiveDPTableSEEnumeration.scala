package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName.*
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.RuleConstant.*
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
 * An implementation of subquery entailment enumeration using a DP table.
 */
final class NaiveDPTableSEEnumeration(
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

    def chaseLocalInstance(localInstance: LocalInstance,
                           namesToBePreservedDuringChase: Set[LocalName]
    ): Set[LocalInstance] = {
      val shortcutChaseOneStep = (instance: LocalInstance) => {
        val localNamesUsableInChildren = {
          (0 until maxArityOfAllPredicatesUsedInRules * 2)
            .map(LocalName(_))
            .toSet -- instance.activeLocalNames
        }.toVector

        // We need to chase the instance with all existential rules
        // while preserving all names in namesToBePreservedDuringChase.
        //
        // A name is preserved by a chase step if and only if
        // it appears in the substituted head of the existential rule.
        //
        // We can first find all possible homomorphisms from the body of
        // the existential rule to the instance by a join algorithm,
        // and then filter out those that do not preserve the names.
        val allChildrenWithRule = (existentialRule: NormalGTGD) => {
          // A set of existential variables in the existential rule
          val existentialVariables = existentialRule.getHead.getBoundVariables.toSet

          // An assignment existential variables into "fresh" local names not used in the parent
          val headVariableHomomorphism =
            existentialVariables.zip(localNamesUsableInChildren).toMap

          FilterNestedLoopJoin[LocalInstanceTerm]
            .join(existentialRule.bodyAsCQ, instance)
            .extendWithConstantHomomorphism(headVariableHomomorphism)
            .materializeFunctionFreeAtom(existentialRule.getHeadAtoms.head)
            .filter((substitutedHead: FormalFact[LocalInstanceTerm]) =>
              // we accept a homomorphism only if names are preserved
              namesToBePreservedDuringChase.subsetOf(
                FormalInstance.of(substitutedHead).activeLocalNames
              )
            )
            .map(substitutedHead => {
              // The instance containing only the head atom produced by the existential rule.
              // This should be a singleton instance because the existential rule is normal.
              val headInstance = FormalInstance.of(substitutedHead)
              val localNamesInHead = headInstance.activeLocalNames

              // the set of facts in the parent instance that are
              // "guarded" by the head of the existential rule
              val inheritedFactsInstance = instance.restrictToAlphabetsWith(term =>
                term.isConstantOrSatisfies(localNamesInHead.contains)
              )

              val saturatedChildInstance = datalogSaturationEngine
                .saturateUnionOfSaturatedAndUnsaturatedInstance(
                  saturatedRuleSet.saturatedRulesAsDatalogProgram,
                  // because the parent is saturated, a restriction of it to the alphabet
                  // occurring in the child is also saturated.
                  inheritedFactsInstance,
                  headInstance
                )

              // we only need to keep chasing with extensional signature, so restrict
              saturatedChildInstance.restrictToSignature(extensionalSignature)
            })
        }

        saturatedRuleSet.existentialRules.flatMap(allChildrenWithRule)
      }

      // we keep chasing from the saturated input instance until we reach a fixpoint
      val saturatedInputInstance = datalogSaturationEngine
        .saturateInstance(
          saturatedRuleSet.saturatedRulesAsDatalogProgram,
          localInstance
        )
      Set(saturatedInputInstance).generateFromElementsUntilFixpoint(shortcutChaseOneStep)
    }

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

      chaseLocalInstance(
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
    //         - `sqei2.coexistentialVariables` equals `sqei1.coexistentialVariables`
    //         - there exists a function θ (a "matching" of local names) that sends active local names
    //           in `sqei1.localInstance` to active terms (so either local names or rule constants)
    //           in `sqei2.localInstance`, such that
    //           - `θ(sqei1.localInstance)` is a subinstance of `sqei2.localInstance`
    //           - `θ . (sqei1.ruleConstantWitnessGuess union sqei1.localWitnessGuess)`
    //             equals `sqei2.ruleConstantWitnessGuess union sqei2.localWitnessGuess` as a map
    //           - `θ . (sqei1.queryConstantEmbedding)` equals `sqei2.queryConstantEmbedding` as a map
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
    //   Out of these four problems,
    //    - Problem 3 has been addressed by NormalizingDPTableSEEnumeration, and
    //    - Problem 4 has been further addressed by DFSNormalizingDPTableSEEnumeration.
    //
    //   The most crucial optimization point is Problem 2, which greatly affects how much
    //   exponential blowup we have to deal with. The challenge is to implement the following:
    //    - dynamically pruning the search space,
    //      i.e. *not even generating* problem instances that are already known to be
    //       - false, which we will not add to the DP table anyway
    //       - true, which is subsumed by some other instance already marked as true
    //    - keeping track of only "maximally subsuming true instances" and "minimally subsuming false instances"
    //    - efficiently matching a problem instance to other subsuming instances using indexing techniques
    val isSubqueryEntailment = isSubqueryEntailmentCached(
      extensionalSignature,
      saturatedRuleSet,
      connectedConjunctiveQuery
    )

    for {
      subqueryEntailmentInstance <-
        NaiveDPTableSEEnumeration.allWellFormedSubqueryEntailmentInstances(
          extensionalSignature,
          saturatedRuleSet.constants,
          connectedConjunctiveQuery
        )
      if isSubqueryEntailment(subqueryEntailmentInstance)
    } yield subqueryEntailmentInstance
  }

  override def toString: String =
    s"NaiveDPTableSEEnumeration{datalogSaturationEngine=$datalogSaturationEngine}"
}

object NaiveDPTableSEEnumeration {
  private def allLocalInstances(extensionalSignature: FunctionFreeSignature,
                                ruleConstants: Set[Constant]
  ): Iterable[LocalInstance] = {
    // We need to consider sufficiently large collection of set of active local names.
    // As it is sufficient to check subquery entailments for all guarded instance
    // over the extensional signature, we only need to consider subsets of
    // {0, ..., 2 * extensionalSignature.maxArity - 1} with size
    // at most extensionalSignature.maxArity.
    val allActiveLocalNameSets = (0 until extensionalSignature.maxArity * 2).map(LocalName(_))
      .toSet
      .powerset
      .filter(_.size <= extensionalSignature.maxArity)

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

  private def allWellFormedSubqueryEntailmentInstances(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: Set[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ) = {
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

      localInstance <- allLocalInstances(extensionalSignature, ruleConstants)
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
