package io.github.kory33.guardedqueries.core.utils.extensions

import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

object ConjunctiveQueryExtensions {
  // We enclose the extension methods in the given instance; see
  // https://github.com/lampepfl/dotty/issues/14777#issuecomment-1078995983 for details
  given Extensions: AnyRef with
    extension (conjunctiveQuery: ConjunctiveQuery)
      /**
       * Computes a subquery of a given `ConjunctiveQuery` that includes only the atoms
       * satisfying a specified predicate.
       *
       * Since `ConjunctiveQuery` cannot be empty, an empty `Optional` is returned if the
       * predicate is not satisfied by any atom in the given `ConjunctiveQuery`.
       */
      def filterAtoms(atomPredicate: Atom => Boolean): Option[ConjunctiveQuery] = {
        val originalFreeVariables = conjunctiveQuery.getFreeVariables.toSet
        val filteredAtoms = conjunctiveQuery.getAtoms.filter(atomPredicate)

        if (filteredAtoms.nonEmpty) {
          // variables in filteredAtoms that are free in the original conjunctiveQuery
          val filteredFreeVariables = filteredAtoms
            .flatMap(_.getVariables)
            .filter(originalFreeVariables.contains)

          Some(ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms))
        } else {
          None
        }
      }

      /**
       * Computes a subquery of a given Conjunctive Query that includes only the atoms that
       * contains at least one bound variable and all bound variables in the atom are present in
       * a specified set of variables.
       *
       * The set of variables in the returned subquery is a subset of `variables`, and a
       * variable is bound in the returned subquery if and only if it is bound in
       * `conjunctiveQuery`.
       *
       * For example, if `conjunctiveQuery` is `∃x,y,z. T(x,y,z) ∧ T(x,y,w) ∧ T(x,c,z)` and
       * `boundVariableSet` is `{x,y`}, then the returned subquery is `∃x,y. T(x,y,w)`.
       *
       * If no atom in the given `ConjunctiveQuery` has variable set entirely contained in
       * `variables`, an empty `Optional` is returned.
       *
       * @param variables
       *   The filter of variables that should be included in the subquery.
       * @return
       *   The computed subquery.
       */
      def strictlyInduceSubqueryByVariables(variables: Set[Variable])
        : Option[ConjunctiveQuery] = {
        val queryBoundVariables = conjunctiveQuery.getBoundVariables.toSet
        conjunctiveQuery.filterAtoms(atom => {
          // variables in the atom that are bound in the CQ
          val atomBoundVariables = atom.getVariables.toSet.intersect(queryBoundVariables)
          atomBoundVariables.subsetOf(variables) && atomBoundVariables.nonEmpty
        })
      }

      /**
       * Computes a subquery of a given Conjunctive Query that includes only the atoms which
       * have at least one bound variable in a specified set of variables.
       *
       * For example, if `conjunctiveQuery` is `∃x,y,z. T(x,y,w) ∧ T(x,c,z)` and
       * `boundVariableSet` is `{y`}, then the returned subquery is `∃x,y. T(x,y,w)`.
       *
       * If no atom in the given `ConjunctiveQuery` has variable set intersecting with
       * `variables`, an empty `Optional` is returned.
       */
      def subqueryRelevantToVariables(variables: Set[Variable]): Option[ConjunctiveQuery] = {
        val queryBoundVariables = conjunctiveQuery.getBoundVariables.toSet
        conjunctiveQuery.filterAtoms(atom => {
          // variables in the atom that are bound in the CQ
          val atomBoundVariables = atom.getVariables.toSet.intersect(queryBoundVariables)
          atomBoundVariables.intersects(variables)
        })
      }

      /**
       * Variables in the strict neighbourhood of a given set of variables in the given CQ.
       *
       * Given a conjunctive query `q` and a variable `x` appearing in `q`, `x` is said to be in
       * the strict neighbourhood of a set `V` of variables if <ol> <li>`x` is not an element of
       * `V`, and</li> <li>`x` occurs in the subquery of `q` relevant to `V`.</li> </ol>
       */
      def strictNeighbourhoodOf(variables: Set[Variable]): Set[Variable] =
        conjunctiveQuery.subqueryRelevantToVariables(variables)
          .map { subquery => subquery.allVariables -- variables }
          .getOrElse(Set.empty)

      /**
       * Given a conjunctive query `conjunctiveQuery` and a set `boundVariables` of variables
       * bound in `conjunctiveQuery`, returns a stream of all `conjunctiveQuery`-connected
       * components of `variables`.
       */
      def connectedComponentsOf(boundVariables: Set[Variable]): Set[Set[Variable]] = {
        if (boundVariables.isEmpty) return Set()

        for (variable <- boundVariables) {
          if (!conjunctiveQuery.getBoundVariables.contains(variable))
            throw new IllegalArgumentException(
              s"Variable $variable is not bound in the given CQ $conjunctiveQuery"
            )
        }

        val unionFindTree = SimpleUnionFindTree(boundVariables)
        for (atom <- conjunctiveQuery.getAtoms) {
          unionFindTree.unionAll(atom.getVariables.toSet.intersect(boundVariables))
        }
        unionFindTree.getEquivalenceClasses
      }

      /**
       * Given a conjunctive query `q` and a set `v` of variables in `q`, checks if `v` is
       * connected in `q`.
       *
       * A set of variables `V` is said to be connected in `q` if there is at most one
       * `q`-connected component of `V` in `q`.
       */
      def connects(variables: Set[Variable]): Boolean =
        conjunctiveQuery.connectedComponentsOf(variables).size <= 1

      def allConstants: Set[Constant] =
        conjunctiveQuery.getTerms.collect { case constant: Constant => constant }.toSet

      def allVariables: Set[Variable] =
        conjunctiveQuery.getBoundVariables.toSet.union(conjunctiveQuery.getFreeVariables.toSet)
}
