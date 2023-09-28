package io.github.kory33.guardedqueries.core.utils.extensions

import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.TGD
import uk.ac.ox.cs.pdq.fol.Variable

object TGDExtensions {
  given Extensions: AnyRef with
    extension (tgd: TGD)
      /**
       * Compute the frontier of the TGD, that is, the set of variables that appear in both the
       * body and the head of the TGD.
       */
      def frontierVariables: Set[Variable] =
        tgd.getHead.getFreeVariables.toSet.intersect(tgd.getBody.getFreeVariables.toSet)

      /**
       * Regard the body of the TGD as a conjunctive query. This method is useful to search
       * homomorphisms with which the TGD can be fired, since searching for such homomorphisms
       * is equivalent to answering the body of the TGD regarded as a conjunctive query.
       */
      def bodyAsCQ: ConjunctiveQuery =
        ConjunctiveQuery.create(tgd.getBody.getFreeVariables, tgd.getBodyAtoms)

      /**
       * Compute the set of universally-quantified variables that do not appear in the head of
       * the TGD.
       */
      def nonFrontierVariables: Set[Variable] =
        tgd.getBody.getFreeVariables.toSet.diff(tgd.getHead.getFreeVariables.toSet)
}
