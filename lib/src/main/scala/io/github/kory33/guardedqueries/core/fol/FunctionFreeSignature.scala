package io.github.kory33.guardedqueries.core.fol

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.utils.extensions.FormulaExtensions
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Formula
import uk.ac.ox.cs.pdq.fol.Predicate
import java.util
import java.util.stream.Collectors

/**
 * An object of this class represents a first-order logic signature with
 *   - countably-infinitely many constants
 *   - finite set of predicate symbols
 *   - no function symbols
 */
object FunctionFreeSignature {
  def fromFormulas(formulas: util.Collection[_ <: Formula]) =
    new FunctionFreeSignature(formulas.stream.flatMap(
      FormulaExtensions.streamPredicatesAppearingIn
    ).collect(Collectors.toList))

  def encompassingRuleQuery(rules: util.Collection[_ <: GTGD],
                            query: ConjunctiveQuery
  ): FunctionFreeSignature = FunctionFreeSignature.fromFormulas(
    ImmutableList.builder[Formula].addAll(rules).add(query).build
  )
}

case class FunctionFreeSignature(predicates: ImmutableSet[Predicate]) {
  def this(predicates: java.lang.Iterable[? <: Predicate]) = {
    this(ImmutableSet.copyOf[Predicate](predicates))
  }

  def predicateNames: ImmutableSet[String] =
    ImmutableSet.copyOf(predicates.stream.map(_.getName).toList)

  def maxArity: Int = predicates.stream.mapToInt(_.getArity).max.orElse(0)
}
