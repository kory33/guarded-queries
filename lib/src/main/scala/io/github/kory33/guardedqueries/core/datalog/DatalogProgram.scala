package io.github.kory33.guardedqueries.core.datalog

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.fol.DatalogRule
import uk.ac.ox.cs.pdq.fol.Dependency
import java.util

case class DatalogProgram(rules: util.Collection[DatalogRule]) {
  override def toString: String = ImmutableList.copyOf(rules).toString
}

object DatalogProgram {
  def tryFromDependencies(dependencies: util.Collection[_ <: Dependency]) =
    new DatalogProgram(dependencies.stream.map(DatalogRule.tryFromDependency).toList)
}
