package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.fol.{DatalogRule, NormalGTGD}
import uk.ac.ox.cs.pdq.fol.Dependency

type DatalogProgram = Set[DatalogRule]

object DatalogProgram {
  def tryFromDependencies(dependencies: Iterable[Dependency]): DatalogProgram =
    dependencies.map(DatalogRule.tryFromDependency).toSet
}

type GuardedDatalogProgram = Set[NormalGTGD.FullGTGD]

object GuardedDatalogProgram {
  def tryFromDependencies(dependencies: Iterable[Dependency]): GuardedDatalogProgram =
    dependencies.map(NormalGTGD.FullGTGD.tryFromDependency).toSet
}
