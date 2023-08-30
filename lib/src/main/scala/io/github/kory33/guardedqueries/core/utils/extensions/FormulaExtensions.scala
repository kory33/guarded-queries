package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Formula
import uk.ac.ox.cs.pdq.fol.Predicate

object FormulaExtensions {
  given Extensions: {} with
    extension (formula: Formula)
      /**
       * Returns the set of predicates appearing in the formula.
       */
      def allPredicates: Set[Predicate] = {
        val children = formula.getChildren[Formula]

        if (children.length == 0) {
          formula match
            case atom: Atom => Set(atom.getPredicate)
            case _ =>
              throw new IllegalArgumentException(
                s"Formula ${formula} is neither an atom nor a composite formula"
              )
        } else {
          children.toSet.flatMap(_.allPredicates)
        }
      }
}
