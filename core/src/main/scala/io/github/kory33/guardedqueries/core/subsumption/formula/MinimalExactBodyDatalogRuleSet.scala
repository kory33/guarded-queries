package io.github.kory33.guardedqueries.core.subsumption.formula

import io.github.kory33.guardedqueries.core.fol.DatalogRule

/**
 * An implementation of [[IndexlessMaximallySubsumingTGDSet]] that keeps track of a set of
 * datalog rules which are "maximal" with respect to the following subsumption relation: A rule
 * R1 subsumes a rule R2 (according to this implementation) if <ul> <li>the body of R1 is a
 * subset of the body of R2</li> <li>the head of R1 is equal to the head of R2</li> </ul>
 */
final class MinimalExactBodyDatalogRuleSet
    extends IndexlessMaximallySubsumingTGDSet[DatalogRule] {
  override protected def firstRuleSubsumesSecond(
    first: DatalogRule,
    second: DatalogRule
  ): Boolean =
    (first.getHeadAtoms sameElements second.getHeadAtoms) &&
      first.getBodyAtoms.toSet.subsetOf(second.getBodyAtoms.toSet)
}
