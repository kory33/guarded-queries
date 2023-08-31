package io.github.kory33.guardedqueries.core.subsumption.formula

import uk.ac.ox.cs.pdq.fol.TGD

/**
 * An interface to objects that can keep track of a set of datalog rules which are "maximal"
 * with respect to a certain subsumption relation.
 *
 * An object conforming to this interface is typically initialized to the "empty" state, and
 * then multiple Datalog rules are then added via [[#addRule]] method. Finally, the set of rules
 * can be retrieved via [[#getRules]] method.
 */
trait MaximallySubsumingTGDSet[F <: TGD] {

  /**
   * Add a rule to the set.
   *
   * The method should check that the rule is not subsumed by any of the rules already in the
   * set, and if it is not, all rules that are subsumed by the new rule should be removed before
   * the new rule is added to the set.
   */
  def addRule(rule: F): Unit

  /**
   * Get the set of rules currently recorded in the set.
   */
  def getRules: Set[F]
}

object MaximallySubsumingTGDSet {
  @FunctionalInterface trait Factory[F <: TGD, +S <: MaximallySubsumingTGDSet[F]] {
    def emptyTGDSet(): S
  }
}
