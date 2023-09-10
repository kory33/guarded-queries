package io.github.kory33.guardedqueries.core.datalog.reversechaseengines

import io.github.kory33.guardedqueries.core.datalog.{
  DatalogSaturationEngine,
  GuardedDatalogProgram,
  GuardedDatalogReverseChaseEngine
}
import io.github.kory33.guardedqueries.core.fol.NormalGTGD.FullGTGD
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.SingleAtomMatching
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.{
  LocalName,
  RuleConstant
}
import io.github.kory33.guardedqueries.core.subsumption.localinstance.MaximallyStrongLocalInstanceSet
import io.github.kory33.guardedqueries.core.subsumption.localinstance.MaximallyStrongLocalInstanceSet.AddResult
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces
import uk.ac.ox.cs.pdq.fol.Variable

private case class ReverseChaseProblemContext(
  localNamesToFix: Set[LocalName],
  program: GuardedDatalogProgram,
  ruleConstantsInProgram: Set[RuleConstant],
  instanceWidthUpperLimit: Int
)

/**
 * Enumerate all possible weakenings of `inputInstance` generated by "weakening maps", i.e. maps
 * of the form `u: LocalName => LocalInstanceTerm` such that
 *   - all `LocalName`s in `range(u)` are fix-points of `u` (i.e. `u` is a unification), and
 *   - all `LocalName`s in `localNamesToFix` are fix-points of `u`.
 */
private def allWeakenings(localNamesToFix: Set[LocalName],
                          ruleConstantsInProgram: Set[RuleConstant]
)(inputInstance: LocalInstance): Iterable[LocalInstance] = {
  import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given

  val unifiableNames = inputInstance.activeLocalNames -- localNamesToFix
  val nonUnifiableTerms = ruleConstantsInProgram ++
    (inputInstance.activeLocalNames intersect localNamesToFix)

  for {
    unification <- unifiableNames.allPartitions
    // This is a choice of which component of `unification` to attach to which non-unifiable term
    componentAttachingMap <- FunctionSpaces
      .allPartialFunctionsBetween(unification, nonUnifiableTerms)
  } yield {
    val termMap: LocalInstanceTerm => LocalInstanceTerm = {
      case unifiableName: LocalName if !localNamesToFix.contains(unifiableName) =>
        // The partition component containing `localName`. This is guaranteed to exist because,
        // by definition of `allPartitions`, `unification` covers all unifiable names.
        val unificationComponent = unification.find(_.contains(unifiableName)).get

        // Either attach the component to a non-unifiable term, or collapse the component
        // to a single local name (namely, the minimum one in the component)
        componentAttachingMap.getOrElse(
          unificationComponent,
          unificationComponent.minBy(_.value)
        )
      case other => other
    }

    inputInstance.map(termMap)
  }
}

/**
 * An implementation of [[GuardedDatalogReverseChaseEngine]] that does not make use of any
 * indexing techniques.
 */
class NaiveReverseChaseEngine(
  saturationEngine: DatalogSaturationEngine,
  localInstanceSetFactory: MaximallyStrongLocalInstanceSet.Factory
) extends GuardedDatalogReverseChaseEngine {

  private def reverseChaseOneStepWith(
    rule: FullGTGD,
    instance: LocalInstance
  )(using ctx: ReverseChaseProblemContext): Iterable[LocalInstance] = {
    import io.github.kory33.guardedqueries.core.formalinstance.CQAsFormalInstance.given

    val ruleVariables = rule.getBodyAtoms.flatMap(_.getVariables).toSet

    // The set of terms to which a reverse-homomorphism may be extended.
    // As the rule is guarded, we will never reduce active local name set by head deletion.
    // Therefore, we can only extend the homomorphism with values from one of
    //  - the set of rule constants
    //  - a set L of local names whose size is `ctx.instanceWidthUpperLimit` and
    //    that contains `instance.activeLocalNames`
    val possibleExtensionRange: Set[LocalInstanceTerm] = ctx.ruleConstantsInProgram ++ {
      val activeLocalNames = instance.activeLocalNames
      activeLocalNames ++ {
        val extraLocalNameCandidates = (0 until ctx.instanceWidthUpperLimit)
          .map(LocalName.apply)
          .toSet

        // "extra" local names not yet active in the current instance
        // but could have been active in (an representative of an isomorphic class of) the parent instance
        (extraLocalNameCandidates -- activeLocalNames).take(
          ctx.instanceWidthUpperLimit - activeLocalNames.size
        )
      }
    }

    def allExtensionsOf(headAtomMatch: HomomorphicMapping[Variable, LocalInstanceTerm])
      : Iterable[HomomorphicMapping[Variable, LocalInstanceTerm]] = {
      val unmappedVariables = ruleVariables -- headAtomMatch.variableOrdering
      FunctionSpaces
        .allFunctionsBetween(unmappedVariables, possibleExtensionRange)
        .map { extension => headAtomMatch.extendWithMap(extension) }
    }

    def deleteHeadAtomsAndAddBodyAtomsWith(
      extendedHomomorphism: HomomorphicMapping[Variable, LocalInstanceTerm]
    ): LocalInstance = {
      val materializedHeadAtoms =
        extendedHomomorphism.materializeFunctionFreeAtoms(rule.getHeadAtoms.toSet)
      val materializedBodyAtoms =
        extendedHomomorphism.materializeFunctionFreeAtoms(rule.getBodyAtoms.toSet)

      FormalInstance(
        instance.facts -- materializedHeadAtoms.facts ++ materializedBodyAtoms.facts
      )
    }

    for {
      weakenedInstance <-
        allWeakenings(ctx.localNamesToFix, ctx.ruleConstantsInProgram)(instance)
      headAtomToErase <- rule.getHeadAtoms
      headAtomMatch <- SingleAtomMatching
        .allMatches(headAtomToErase.asQueryLikeAtom[LocalInstanceTerm], weakenedInstance)
        .allHomomorphisms
      extendedHomomorphism <- allExtensionsOf(headAtomMatch)
      reverseChased = deleteHeadAtomsAndAddBodyAtomsWith(extendedHomomorphism)
      if ctx.localNamesToFix subsetOf reverseChased.activeLocalNames
    } yield reverseChased
  }

  private def performDFS(
    currentInstance: LocalInstance,
    maximalInstancesSoFar: MaximallyStrongLocalInstanceSet
  )(using ctx: ReverseChaseProblemContext): Unit = {
    for {
      rule <- ctx.program
      reverseChasedOneStep <-
        reverseChaseOneStepWith(rule, currentInstance)
    } {
      if (maximalInstancesSoFar.add(reverseChasedOneStep) == AddResult.Added) {
        // we only go further up the tree if the reverse-chased instance is not a subsumed one
        performDFS(reverseChasedOneStep, maximalInstancesSoFar)
      }
    }
  }

  override def reverseFullChase(
    localNamesToFix: Set[LocalName],
    program: GuardedDatalogProgram,
    instanceWidthUpperLimit: Int,
    inputInstance: LocalInstance
  ): Iterable[LocalInstance] = {
    val saturatedInputInstance: LocalInstance =
      saturationEngine.saturateInstance(program.map(_.asDatalogRule), inputInstance)

    val maximalInstanceSet = localInstanceSetFactory.newSet(localNamesToFix)

    given ReverseChaseProblemContext =
      ReverseChaseProblemContext(
        localNamesToFix,
        program,
        program.flatMap(_.allConstants).map(RuleConstant.apply),
        instanceWidthUpperLimit
      )

    performDFS(saturatedInputInstance, maximalInstanceSet)

    maximalInstanceSet.getMaximalLocalInstances
  }
}