package io.github.kory33.guardedqueries.core.fol

import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.*

import scala.jdk.CollectionConverters.*

abstract sealed class NormalGTGD protected (body: Set[Atom], head: Set[Atom])
    extends GTGD(body.asJava, head.asJava) {
  def asSingleHeaded: NormalGTGD.SingleHeadedGTGD = this match {
    case singleHeaded: NormalGTGD.SingleHeadedGTGD => singleHeaded
    case _ =>
      throw new IllegalArgumentException(
        "Cannot convert a non-single-headed GTGD into a single-headed one"
      )
  }
}

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
  class SingleHeadedGTGD(body: Set[Atom], val headAtom: Atom)
      extends NormalGTGD(body, Set(headAtom)) {}

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

  object FullGTGD {

    /**
     * Try constructing the a full GTGD from a dependency.
     *
     * This function may throw an [[IllegalArgumentException]] if the given dependency contains
     * existentially quantified variables or is not guarded.
     */
    def tryFromDependency(dependency: Dependency) =
      new NormalGTGD.FullGTGD(dependency.getBodyAtoms.toSet, dependency.getHeadAtoms.toSet)

    given Extensions: AnyRef with
      extension (gtgd: FullGTGD)
        def asDatalogRule: DatalogRule = DatalogRule(gtgd.getBodyAtoms, gtgd.getHeadAtoms)
        def allConstants: Set[Constant] = gtgd.getTerms.collect { case c: Constant => c }.toSet
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
    val fullGTGDs = fullRules.map(FullGTGD.tryFromDependency)

    val splitExistentialRules = existentialRules.zipWithIndex.flatMap { (originalRule, index) =>
      val intermediaryPredicateVariables =
        (originalRule.frontierVariables ++ originalRule.getExistential).toArray

      val intermediaryPredicate: Predicate = Predicate.create(
        s"${intermediaryPredicatePrefix}_$index",
        intermediaryPredicateVariables.length
      )

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
