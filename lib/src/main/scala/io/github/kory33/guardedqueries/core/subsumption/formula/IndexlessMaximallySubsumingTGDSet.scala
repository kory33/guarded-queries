package io.github.kory33.guardedqueries.core.subsumption.formula

import uk.ac.ox.cs.pdq.fol.TGD
import scala.collection.mutable

/**
 * A partial implementation of [[MaximallySubsumingTGDSet]] that does not use any index to
 * filter out subsumption candidates.
 */
abstract class IndexlessMaximallySubsumingTGDSet[F <: TGD] extends MaximallySubsumingTGDSet[F] {

  /**
   * Judge if the first rule subsumes the second rule, according to an implementation-specific
   * subsumption relation.
   */
  protected def firstRuleSubsumesSecond(first: F, second: F): Boolean

  final private val rulesKnownToBeMaximalSoFar = mutable.HashSet.empty[F]

  private def isSubsumedByExistingRule(rule: F) =
    rulesKnownToBeMaximalSoFar.exists(firstRuleSubsumesSecond(_, rule))

  override final def addRule(rule: F): Unit = {
    if (!isSubsumedByExistingRule(rule)) {
      rulesKnownToBeMaximalSoFar.filterInPlace(!firstRuleSubsumesSecond(rule, _))
      rulesKnownToBeMaximalSoFar.add(rule)
    }
  }

  override final def getRules: Set[F] = rulesKnownToBeMaximalSoFar.toSet
}
