package io.github.kory33.guardedqueries.core.fol

import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{Atom, Predicate, Variable}

import java.util

abstract sealed class NormalGTGD protected (body: util.Set[Atom], head: util.Set[Atom])
    extends GTGD(body, head) {}

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
  class SingleHeadedGTGD(body: util.Set[Atom], head: Atom)
      extends NormalGTGD(body, util.Set.of(head)) {}

  /**
   * An existential-free GTGD.
   *
   * The primary constructor throws an [[IllegalArgumentException]] if not all variables in the
   * head appear in the body.
   */
  class FullGTGD(body: util.Collection[Atom], head: util.Collection[Atom])
      extends NormalGTGD(ImmutableSet.copyOf(body), ImmutableSet.copyOf(head)) {
    if (getExistential.length != 0) throw new IllegalArgumentException(
      "Datalog rules cannot contain existential variables, got " + super.toString
    )
  }

  object FullGTGD {

    /**
     * Try constructing the object from a GTGD.
     *
     * This function may throw an [[IllegalArgumentException]] if the given GTGD contains
     * existentially quantified variables.
     */
    def tryFromGTGD(gtgd: GTGD) =
      new NormalGTGD.FullGTGD(
        util.Set.of(gtgd.getBodyAtoms: _*),
        util.Set.of(gtgd.getHeadAtoms: _*)
      )
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
  def normalize(inputRules: util.Collection[_ <: GTGD],
                intermediaryPredicatePrefix: String
  ): ImmutableSet[NormalGTGD] = {
    import scala.jdk.CollectionConverters.*

    val (fullRules, existentialRules) =
      inputRules.asScala.toList.partition(_.getExistential.length == 0)
    val fullGTGDs = fullRules.map(FullGTGD.tryFromGTGD)

    val splitExistentialRules =
      existentialRules.zipWithIndex.flatMap { (originalRule, index) =>
        val intermediaryPredicateVariables = {
          val frontierVariables = TGDExtensions.frontierVariables(originalRule)

          (frontierVariables.asScala.toList ++ originalRule.getExistential.toList).toSet.toArray
        }

        val intermediaryPredicate: Predicate = {
          val predicateName =
            intermediaryPredicatePrefix + "_" + Integer.toUnsignedString(index)
          val predicateArity = intermediaryPredicateVariables.length
          Predicate.create(predicateName, predicateArity)
        }

        val splitExistentialRule = NormalGTGD.SingleHeadedGTGD(
          util.Set.of(originalRule.getBodyAtoms: _*),
          Atom.create(intermediaryPredicate, intermediaryPredicateVariables: _*)
        )

        val splitFullRule = NormalGTGD.FullGTGD(
          util.Set.of(Atom.create(intermediaryPredicate, intermediaryPredicateVariables: _*)),
          util.Set.of(originalRule.getHeadAtoms: _*)
        )

        List(splitExistentialRule, splitFullRule)
      }

    ImmutableSet.copyOf((fullGTGDs ++ splitExistentialRules).asJava)
  }
}
