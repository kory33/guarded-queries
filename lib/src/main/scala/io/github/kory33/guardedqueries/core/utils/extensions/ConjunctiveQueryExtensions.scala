package io.github.kory33.guardedqueries.core.utils.extensions

import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

object ConjunctiveQueryExtensions {

  /**
   * Computes a subquery of a given {@code ConjunctiveQuery} that includes only the atoms
   * satisfying a specified predicate. <p> Since {@code ConjunctiveQuery} cannot be empty, an
   * empty {@code Optional} is returned if the predicate is not satisfied by any atom in the
   * given {@code ConjunctiveQuery}.
   */
  def filterAtoms(conjunctiveQuery: ConjunctiveQuery)(atomPredicate: Atom => Boolean)
    : Option[ConjunctiveQuery] = {
    val originalFreeVariables = conjunctiveQuery.getFreeVariables.toSet
    val filteredAtoms = conjunctiveQuery.getAtoms.filter(atomPredicate)

    if (filteredAtoms.isEmpty) {
      None
    } else {
      // variables in filteredAtoms that are free in the original conjunctiveQuery
      val filteredFreeVariables = filteredAtoms
        .flatMap(_.getVariables)
        .filter(originalFreeVariables.contains)

      Some(ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms))
    }
  }

  /**
   * Computes a subquery of a given Conjunctive Query that includes only the atoms that contains
   * at least one bound variable and all bound variables in the atom are present in a specified
   * set of variables. <p> The set of variables in the returned subquery is a subset of {@code
   * variables}, and a variable is bound in the returned subquery if and only if it is bound in
   * {@code conjunctiveQuery}. <p> For example, if {@code conjunctiveQuery} is {@code ∃x,y,z.
   * T(x,y,z) ∧ T(x,y,w) ∧ T(x,c,z)} and {@code boundVariableSet} is {@code {x,y}}, then the
   * returned subquery is {@code ∃x,y. T(x,y,w)}. <p> If no atom in the given {@code
   * ConjunctiveQuery} has variable set entirely contained in {@code variables}, an empty {@code
   * Optional} is returned.
   *
   * @param conjunctiveQuery
   *   The Conjunctive Query to compute the subquery from
   * @param variables
   *   The filter of variables that should be included in the subquery.
   * @return
   *   The computed subquery.
   */
  def strictlyInduceSubqueryByVariables(variables: Set[Variable])(
    conjunctiveQuery: ConjunctiveQuery
  ): Option[ConjunctiveQuery] = {
    val queryBoundVariables = conjunctiveQuery.getBoundVariables.toSet
    filterAtoms(conjunctiveQuery)(atom => {
      // variables in the atom that are bound in the CQ
      val atomBoundVariables = atom.getVariables.toSet.intersect(queryBoundVariables)
      atomBoundVariables.subsetOf(variables) && atomBoundVariables.nonEmpty
    })
  }

  /**
   * Computes a subquery of a given Conjunctive Query that includes only the atoms which have at
   * least one bound variable in a specified set of variables. <p> For example, if {@code
   * conjunctiveQuery} is {@code ∃x,y,z. T(x,y,w) ∧ T(x,c,z)} and {@code boundVariableSet} is
   * {@code {y}}, then the returned subquery is {@code ∃x,y. T(x,y,w)}. <p> If no atom in the
   * given {@code ConjunctiveQuery} has variable set intersecting with {@code variables}, an
   * empty {@code Optional} is returned.
   */
  def subqueryRelevantToVariables(variables: Set[Variable])(conjunctiveQuery: ConjunctiveQuery)
    : Option[ConjunctiveQuery] = {
    val queryBoundVariables = conjunctiveQuery.getBoundVariables.toSet
    filterAtoms(conjunctiveQuery)(atom => {
      // variables in the atom that are bound in the CQ
      val atomBoundVariables = atom.getVariables.toSet.intersect(queryBoundVariables)
      atomBoundVariables.exists(variables.contains)
    })
  }

  /**
   * Variables in the strict neighbourhood of a given set of variables in the given CQ. <p>
   * Given a conjunctive query {@code q} and a variable {@code x} appearing in {@code q}, {@code
   * x} is said to be in the strict neighbourhood of a set {@code V} of variables if <ol>
   * <li>{@code x} is not an element of {@code V}, and</li> <li>{@code x} occurs in the subquery
   * of {@code q} relevant to {@code V}.</li> </ol>
   */
  def neighbourhoodVariables(
    conjunctiveQuery: ConjunctiveQuery,
    variables: Set[Variable]
  ): Set[Variable] =
    subqueryRelevantToVariables(variables)(conjunctiveQuery)
      .map { subquery => variablesIn(subquery) -- variables }
      .getOrElse(Set.empty)

  /**
   * Given a conjunctive query {@code conjunctiveQuery} and a set {@code boundVariables} of
   * variables bound in {@code conjunctiveQuery}, returns a stream of all {@code
   * conjunctiveQuery}-connected components of {@code variables}.
   */
  def connectedComponents(conjunctiveQuery: ConjunctiveQuery,
                          boundVariables: Set[Variable]
  ): Set[Set[Variable]] = {
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
   * Given a conjunctive query {@code q} and a set {@code v} of variables in {@code q}, checks
   * if {@code v} is connected in {@code q}. <p> A set of variables {@code V} is said to be
   * connected in {@code q} if there is at most one {@code q}-connected component of {@code V}
   * in {@code q}.
   */
  def isConnected(
    conjunctiveQuery: ConjunctiveQuery,
    variables: Set[Variable]
  ): Boolean = connectedComponents(conjunctiveQuery, variables).size <= 1

  def constantsIn(conjunctiveQuery: ConjunctiveQuery): Set[Constant] =
    conjunctiveQuery.getTerms.flatMap {
      case constant: Constant => Some(constant)
      case _                  => None
    }.toSet

  def variablesIn(conjunctiveQuery: ConjunctiveQuery): Set[Variable] =
    conjunctiveQuery.getBoundVariables.toSet.union(conjunctiveQuery.getFreeVariables.toSet)
}
