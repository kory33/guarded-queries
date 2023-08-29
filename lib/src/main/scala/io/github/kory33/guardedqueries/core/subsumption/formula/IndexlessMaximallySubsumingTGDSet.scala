package io.github.kory33.guardedqueries.core.subsumption.formula

import uk.ac.ox.cs.pdq.fol.TGD
import java.util

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
  final private val rulesKnownToBeMaximalSoFar = new util.HashSet[F]
  private def isSubsumedByExistingRule(rule: F) = rulesKnownToBeMaximalSoFar.stream.anyMatch(
    (existingRule: F) => firstRuleSubsumesSecond(existingRule, rule)
  )
  override final def addRule(rule: F): Unit = {
    if (!isSubsumedByExistingRule(rule)) {
      rulesKnownToBeMaximalSoFar.removeIf((existingRule: F) =>
        firstRuleSubsumesSecond(rule, existingRule)
      )
      rulesKnownToBeMaximalSoFar.add(rule)
    }
  }
  override final def getRules: Set[F] = Set.copyOf(rulesKnownToBeMaximalSoFar)
}
