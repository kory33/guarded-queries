package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import uk.ac.ox.cs.pdq.fol.Constant

trait DatalogSaturationEngine {
  def saturateInstance[TA](program: DatalogProgram,
                           instance: FormalInstance[TA],
                           includeConstantsToTA: Constant => TA
  ): FormalInstance[TA] = this.saturateUnionOfSaturatedAndUnsaturatedInstance(
    program,
    FormalInstance.empty,
    instance,
    includeConstantsToTA
  )

  /**
   * Saturates the union of the given saturated instance and the given unsaturated instance.
   */
  def saturateUnionOfSaturatedAndUnsaturatedInstance[TA](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA],
    includeConstantsToTA: Constant => TA
  ): FormalInstance[TA]
}
