package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Variable

object VariableSetExtensions {
  given Extension: {} with
    extension (variables: Set[Variable])
      /**
       * Sort the given set of variables by their lexicographical order.
       */
      def sortBySymbol: List[Variable] = {
        variables.toList.sortBy(_.getSymbol)
      }
}
