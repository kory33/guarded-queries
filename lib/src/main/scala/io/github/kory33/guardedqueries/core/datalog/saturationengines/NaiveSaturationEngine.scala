package io.github.kory33.guardedqueries.core.datalog.saturationengines

import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.IncludesFolConstants
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given

/**
 * An implementation of [[DatalogSaturationEngine]] that performs naive bottom-up saturation.
 */
class NaiveSaturationEngine extends DatalogSaturationEngine {

  /**
   * Produce a collection of all facts that can be derived from the given set of facts using the
   * given datalog program once.
   */
  private def chaseSingleStep[TA: IncludesFolConstants](
    program: DatalogProgram,
    facts: Set[FormalFact[TA]]
  ) = {
    val inputInstance = FormalInstance[TA](facts)
    val joinAlgorithm = FilterNestedLoopJoin[TA]

    for {
      rule <- program.rules
      joinResult = joinAlgorithm.join(rule.bodyAsCQ, inputInstance)
      ruleHeadAtom <- rule.getHeadAtoms
      // because we are dealing with Datalog rules, we can materialize every head atom
      // using the join result (and its variable ordering)
      materializedHead <-
        joinResult.materializeFunctionFreeAtom(ruleHeadAtom)
    } yield materializedHead
  }

  override def saturateUnionOfSaturatedAndUnsaturatedInstance[TA: IncludesFolConstants](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA]
  ): FormalInstance[TA] = FormalInstance[TA] {
    (saturatedInstance.facts ++ instance.facts)
      .generateFromSetUntilFixpoint(chaseSingleStep(program, _))
  }
}
