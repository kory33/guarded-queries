package io.github.kory33.guardedqueries.core.fol

import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import scala.jdk.CollectionConverters.*

abstract sealed class NormalGTGD protected (body: Set[Atom], head: Set[Atom])
    extends GTGD(body.asJava, head.asJava) {}

/**
 * A GTGD in a normal form (i.e. either single-headed or full).
 *
 * Any GTGD can be transformed into a pair of GTGDs in normal forms by extending the language.
 * For details, see [[normalize]] for more details.
 */
object NormalGTGD {

  /**
   * A single-headed GTGD.
   */
  private class SingleHeadedGTGD(body: Set[Atom], head: Atom)
      extends NormalGTGD(body, Set(head)) {}

  /**
   * An existential-free GTGD.
   *
   * The primary constructor throws an [[IllegalArgumentException]] if not all variables in the
   * head appear in the body.
   */
  class FullGTGD(body: Set[Atom], head: Set[Atom]) extends NormalGTGD(body, head) {
    if (getExistential.length != 0) throw new IllegalArgumentException(
      "Datalog rules cannot contain existential variables, got " + super.toString
    )
  }

  private object FullGTGD {

    /**
     * Try constructing the object from a GTGD.
     *
     * This function may throw an [[IllegalArgumentException]] if the given GTGD contains
     * existentially quantified variables.
     */
    def tryFromGTGD(gtgd: GTGD) =
      new NormalGTGD.FullGTGD(gtgd.getBodyAtoms.toSet, gtgd.getHeadAtoms.toSet)
  }

  /**
   * Normalize a collection of GTGDs.
   *
   * Any GTGD can be transformed into a pair of GTGDs in normal forms by extending the language.
   * For example, a GTGD
   *
   * <pre>∀x,y. R(x,y) -> ∃z,w. S(x,y,z) ∧ T(y,y,w)</pre>
   *
   * can be "split" into two GTGDs by introducing a fresh intermediary predicate `I(-,-,-,-)`:
   *
   *   1. <code>∀x,y. R(x,y) -> ∃z,w. I(x,y,z,w)</code>, and
   *   1. <code>∀x,y,z,w. I(x,y,z,w) → S(x,y,z) ∧ T(y,y,w)</code>.
   *
   * @param inputRules
   *   a collection of rules to normalize
   * @param intermediaryPredicatePrefix
   *   a prefix to use for naming intermediary predicates. For example, if the prefix is "I",
   *   intermediary predicates will have names "I_0", "I_1", etc.
   */
  def normalize(inputRules: Set[GTGD], intermediaryPredicatePrefix: String): Set[NormalGTGD] = {
    val (fullRules, existentialRules) =
      inputRules.partition(_.getExistential.length == 0)
    val fullGTGDs = fullRules.map(FullGTGD.tryFromGTGD)

    val splitExistentialRules =
      existentialRules.zipWithIndex.flatMap { (originalRule, index) =>
        val intermediaryPredicateVariables =
          (originalRule.frontierVariables ++ originalRule.getExistential).toArray

        val intermediaryPredicate: Predicate = {
          val predicateName =
            intermediaryPredicatePrefix + "_" + Integer.toUnsignedString(index)
          val predicateArity = intermediaryPredicateVariables.length
          Predicate.create(predicateName, predicateArity)
        }

        val splitExistentialRule = NormalGTGD.SingleHeadedGTGD(
          originalRule.getBodyAtoms.toSet,
          Atom.create(intermediaryPredicate, intermediaryPredicateVariables: _*)
        )

        val splitFullRule = NormalGTGD.FullGTGD(
          Set(Atom.create(intermediaryPredicate, intermediaryPredicateVariables: _*)),
          originalRule.getHeadAtoms.toSet
        )

        List(splitExistentialRule, splitFullRule)
      }

    fullGTGDs ++ splitExistentialRules
  }
}
