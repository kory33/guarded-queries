package io.github.kory33.guardedqueries.core.fol

import io.github.kory33.guardedqueries.core.utils.extensions.FormulaExtensions
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Formula, Predicate}
import io.github.kory33.guardedqueries.core.utils.extensions.FormulaExtensions.given

/**
 * An object of this class represents a first-order logic signature with
 *   - countably-infinitely many constants
 *   - finite set of predicate symbols
 *   - no function symbols
 */
object FunctionFreeSignature {
  private def fromFormulas(formulas: Set[Formula]) =
    new FunctionFreeSignature(formulas.flatMap(_.allPredicates))

  def encompassingRuleQuery(rules: Set[GTGD], query: ConjunctiveQuery): FunctionFreeSignature =
    FunctionFreeSignature.fromFormulas(Set(query: Formula) ++ rules)
}

case class FunctionFreeSignature(predicates: Set[Predicate]) {
  def this(predicates: Iterable[Predicate]) = {
    this(predicates.toSet)
  }

  def predicateNames: Set[String] =
    predicates.map(_.getName)

  def maxArity: Int = predicates.map(_.getArity) match {
    case arities if arities.isEmpty => 0
    case arities                    => arities.max
  }
}
