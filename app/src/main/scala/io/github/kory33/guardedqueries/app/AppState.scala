package io.github.kory33.guardedqueries.app

import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.Dependency

case class AppState(registeredRules: Set[GTGD]) {
  def registerRule(rule: GTGD): AppState = AppState(registeredRules + rule)
  def registeredRulesAsDependencies: Set[Dependency] = registeredRules.map(r => r)
}

object AppState {
  def empty: AppState = new AppState(Set.empty)
}
