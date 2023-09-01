package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Variable

object VariableIterableExtensions {
  given Extension: AnyRef with
    extension (variables: Iterable[Variable])
      /**
       * Sort the given set of variables by their lexicographical order.
       */
      def sortBySymbol: Vector[Variable] = {
        variables.toVector.sortBy(_.getSymbol)
      }
}
