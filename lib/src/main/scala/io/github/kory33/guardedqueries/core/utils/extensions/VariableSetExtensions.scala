package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableList
import uk.ac.ox.cs.pdq.fol.Variable
import java.util
import java.util.Comparator

object VariableSetExtensions {

  /**
   * Deduplicate and sort the given collection of variables by their lexicographical order.
   */
  def sortBySymbol[V <: Variable](variables: util.Collection[V]): ImmutableList[Variable] = {
    val sortedVariables =
      new util.ArrayList(new util.HashSet(variables))

    sortedVariables.sort(Comparator.comparing((v: Variable) => v.getSymbol))

    ImmutableList.copyOf(sortedVariables)
  }
}
