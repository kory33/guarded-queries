package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.fol.DatalogRule
import uk.ac.ox.cs.pdq.fol.Dependency

type DatalogProgram = Set[DatalogRule]

object DatalogProgram {
  def tryFromDependencies(dependencies: Iterable[Dependency]): DatalogProgram =
    dependencies.map(DatalogRule.tryFromDependency).toSet
}
