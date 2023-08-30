package io.github.kory33.guardedqueries.core.datalog.saturationengines

import io.github.kory33.guardedqueries.core.datalog.{DatalogProgram, DatalogSaturationEngine}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.utils.extensions.{SetLikeExtensions, TGDExtensions}
import uk.ac.ox.cs.pdq.fol.Constant

import java.util
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
    val inputInstance = new FormalInstance[TA](facts)
    val producedFacts = mutable.HashSet[FormalFact[TA]]()
    val joinAlgorithm = new FilterNestedLoopJoin[TA](includeConstantsToTA)

    for (rule <- program.rules) {
      val joinResult = joinAlgorithm.join(TGDExtensions.bodyAsCQ(rule), inputInstance)
      for (ruleHeadAtom <- rule.getHeadAtoms) {
        producedFacts ++= {
          // because we are dealing with Datalog rules, we can materialize every head atom
          // using the join result (and its variable ordering)
          joinResult.materializeFunctionFreeAtom(ruleHeadAtom, includeConstantsToTA)
        }
      }
    }

    producedFacts.toSet
  }

  override def saturateUnionOfSaturatedAndUnsaturatedInstance[TA](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA],
    includeConstantsToTA: Constant => TA
  ): FormalInstance[TA] = {
    val saturatedFactSet = SetLikeExtensions.generateFromSetUntilFixpoint(
      saturatedInstance.facts ++ instance.facts,
      (facts: Set[FormalFact[TA]]) =>
        chaseSingleStep(program, facts, includeConstantsToTA)
    )
    new FormalInstance[TA](saturatedFactSet)
  }
}
