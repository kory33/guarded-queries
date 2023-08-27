package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Formula
import uk.ac.ox.cs.pdq.fol.Predicate
import java.util.stream.Stream

object FormulaExtensions {
  def streamPredicatesAppearingIn(formula: Formula): Stream[Predicate] = {
    val children = formula.getChildren
    if (children.length == 0) if (formula.isInstanceOf[Atom]) Stream.of(atom.getPredicate)
    else throw new IllegalArgumentException(
      "Formula " + formula + "is neither an atom nor a composite formula"
    )
    else Stream.of(children).flatMap(FormulaExtensions.streamPredicatesAppearingIn)
  }
}
