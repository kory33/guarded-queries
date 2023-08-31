package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.TGD
import uk.ac.ox.cs.pdq.fol.Variable

object TGDExtensions {
  given Extensions: AnyRef with
    extension (tgd: TGD)
      /**
       * Compute the frontier of a given TGD, that is, the set of variables that appear in both
       * the body and the head of the TGD.
       */
      def frontierVariables: Set[Variable] =
        tgd.getHead.getFreeVariables.toSet.intersect(tgd.getBody.getFreeVariables.toSet)

      def bodyAsCQ: ConjunctiveQuery =
        ConjunctiveQuery.create(tgd.getBody.getFreeVariables, tgd.getBodyAtoms)
}
