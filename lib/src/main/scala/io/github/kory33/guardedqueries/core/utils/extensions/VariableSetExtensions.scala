package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.Variable

object VariableSetExtensions {

  /**
   * Sort the given set of variables by their lexicographical order.
   */
  def sortBySymbol(variables: Set[Variable]): List[Variable] = {
    variables.toList.sortBy(_.getSymbol)
  }
}
