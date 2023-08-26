package io.github.kory33.guardedqueries.core.datalog.saturationengines

import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions
import uk.ac.ox.cs.pdq.fol.Constant
import java.util
import java.util.function.Function

/**
 * An implementation of [[DatalogSaturationEngine]] that performs naive bottom-up saturation.
 */
class NaiveSaturationEngine extends DatalogSaturationEngine {

  /**
   * Produce a collection of all facts that can be derived from the given set of facts using the
   * given datalog program once.
   */
  private def chaseSingleStep[TA](program: DatalogProgram,
                                  facts: ImmutableSet[FormalFact[TA]],
                                  includeConstantsToTA: Function[Constant, TA]
  ) = {
    val inputInstance = new FormalInstance[TA](facts)
    val producedFacts = new util.HashSet[FormalFact[TA]]
    val joinAlgorithm = new FilterNestedLoopJoin[TA](includeConstantsToTA)
    import scala.jdk.CollectionConverters._

    for (rule <- program.rules.asScala) {
      val joinResult = joinAlgorithm.join(TGDExtensions.bodyAsCQ(rule), inputInstance)
      for (ruleHeadAtom <- rule.getHeadAtoms) {
        producedFacts.addAll(
          // because we are dealing with Datalog rules, we can materialize every head atom
          // using the join result (and its variable ordering)
          joinResult.materializeFunctionFreeAtom(ruleHeadAtom, includeConstantsToTA)
        )
      }
    }
    producedFacts
  }
  override def saturateUnionOfSaturatedAndUnsaturatedInstance[TA](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA],
    includeConstantsToTA: Function[Constant, TA]
  ): FormalInstance[TA] = {
    val saturatedFactSet = SetLikeExtensions.generateFromSetUntilFixpoint(
      SetLikeExtensions.union(saturatedInstance.facts, instance.facts),
      (facts: ImmutableSet[FormalFact[TA]]) =>
        chaseSingleStep(program, facts, includeConstantsToTA)
    )
    new FormalInstance[TA](saturatedFactSet)
  }
}
