package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.formalinstance.{
  FormalInstance,
  IncludesFolConstants
}

trait DatalogSaturationEngine {
  def saturateInstance[TA: IncludesFolConstants](
    program: DatalogProgram,
    instance: FormalInstance[TA]
  ): FormalInstance[TA] =
    saturateUnionOfSaturatedAndUnsaturatedInstance(program, FormalInstance.empty, instance)

  /**
   * Saturates the union of the given saturated instance and the given unsaturated instance.
   */
  def saturateUnionOfSaturatedAndUnsaturatedInstance[TA: IncludesFolConstants](
    program: DatalogProgram,
    saturatedInstance: FormalInstance[TA],
    instance: FormalInstance[TA]
  ): FormalInstance[TA]
}
