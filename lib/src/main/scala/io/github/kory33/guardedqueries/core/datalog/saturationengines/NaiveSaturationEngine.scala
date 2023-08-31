package io.github.kory33.guardedqueries.core.datalog.saturationengines

import io.github.kory33.guardedqueries.core.datalog.{DatalogProgram, DatalogSaturationEngine}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given

import uk.ac.ox.cs.pdq.fol.Constant

import scala.collection.mutable

/**
 * An implementation of [[DatalogSaturationEngine]] that performs naive bottom-up saturation.
 */
class NaiveSaturationEngine extends DatalogSaturationEngine {

  /**
   * Produce a collection of all facts that can be derived from the given set of facts using the
   * given datalog program once.
   */
  private def chaseSingleStep[TA](program: DatalogProgram,
                                  facts: Set[FormalFact[TA]],
                                  includeConstantsToTA: Constant => TA
  ) = {
    val inputInstance = FormalInstance[TA](facts)
    val joinAlgorithm = FilterNestedLoopJoin[TA](includeConstantsToTA)

    for {
      rule <- program.rules
      joinResult = joinAlgorithm.join(rule.bodyAsCQ, inputInstance)
      ruleHeadAtom <- rule.getHeadAtoms
      // because we are dealing with Datalog rules, we can materialize every head atom
      // using the join result (and its variable ordering)
      materializedHead <-
        joinResult.materializeFunctionFreeAtom(ruleHeadAtom, includeConstantsToTA)
    } yield materializedHead
  }

  override def saturateUnionOfSaturatedAndUnsaturatedInstance[TA](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA],
    includeConstantsToTA: Constant => TA
  ): FormalInstance[TA] = FormalInstance[TA] {
    (saturatedInstance.facts ++ instance.facts)
      .generateFromSetUntilFixpoint(chaseSingleStep(program, _, includeConstantsToTA))
  }
}
