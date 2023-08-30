package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.fol.DatalogRule
import uk.ac.ox.cs.pdq.fol.Dependency

case class DatalogProgram(rules: Set[DatalogRule]) {
  override def toString: String = rules.toString()
}

object DatalogProgram {
  def tryFromDependencies(dependencies: Iterable[Dependency]): DatalogProgram =
    DatalogProgram(dependencies.map(DatalogRule.tryFromDependency).toSet)
}
