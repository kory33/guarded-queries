package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Formula
import uk.ac.ox.cs.pdq.fol.Predicate
import java.util.stream.Stream

object FormulaExtensions {
  def streamPredicatesAppearingIn(formula: Formula): Stream[Predicate] = {
    val children = formula.getChildren[Formula]
    if (children.length == 0) {
      formula match {
        case atom: Atom => Stream.of(atom.getPredicate)
        case _ => throw new IllegalArgumentException(
            s"Formula ${formula} is neither an atom nor a composite formula"
          )
      }
    } else {
      Stream.of(children: _*).flatMap(streamPredicatesAppearingIn(_))
    }
  }
}
