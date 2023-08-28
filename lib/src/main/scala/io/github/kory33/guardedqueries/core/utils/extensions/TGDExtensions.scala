package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableSet
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.TGD
import uk.ac.ox.cs.pdq.fol.Variable
import java.util

object TGDExtensions {

  /**
   * Compute the frontier of a given TGD, that is, the set of variables that appear in both the
   * body and the head of the TGD.
   */
  def frontierVariables(tgd: TGD): ImmutableSet[Variable] = {
    val headVariables = tgd.getHead.getFreeVariables
    val bodyVariables = tgd.getBody.getFreeVariables
    SetLikeExtensions.intersection(
      util.Arrays.asList(headVariables: _*),
      util.Arrays.asList(bodyVariables: _*)
    )
  }

  def bodyAsCQ(tgd: TGD): ConjunctiveQuery = {
    val bodyAtoms = tgd.getBodyAtoms
    val bodyVariables = tgd.getBody.getFreeVariables
    ConjunctiveQuery.create(bodyVariables, bodyAtoms)
  }
}
