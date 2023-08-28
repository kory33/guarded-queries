package io.github.kory33.guardedqueries.core.utils.extensions

import scala.jdk.CollectionConverters.*
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import java.util
import java.util.Optional
import java.util.function.Predicate
import java.util.stream.Stream

object ConjunctiveQueryExtensions {

  /**
   * Computes a subquery of a given {@code ConjunctiveQuery} that includes only the atoms
   * satisfying a specified predicate. <p> Since {@code ConjunctiveQuery} cannot be empty, an
   * empty {@code Optional} is returned if the predicate is not satisfied by any atom in the
   * given {@code ConjunctiveQuery}.
   */
  def filterAtoms(conjunctiveQuery: ConjunctiveQuery,
                  atomPredicate: Predicate[_ >: Atom]
  ): Optional[ConjunctiveQuery] = {
    val originalFreeVariables = conjunctiveQuery.getFreeVariables.toSet
    val filteredAtoms = conjunctiveQuery.getAtoms.filter(atomPredicate.test)

    if (filteredAtoms.isEmpty) {
      Optional.empty
    } else {
      // variables in filteredAtoms that are free in the original conjunctiveQuery
      val filteredFreeVariables = filteredAtoms
        .flatMap(_.getVariables)
        .filter(originalFreeVariables.contains)

      Optional.of(ConjunctiveQuery.create(filteredFreeVariables, filteredAtoms))
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
  def strictlyInduceSubqueryByVariables(conjunctiveQuery: ConjunctiveQuery,
                                        variables: util.Collection[_ <: Variable]
  ): Optional[ConjunctiveQuery] = {
    val variableSet = ImmutableSet.copyOf[Variable](variables)
    filterAtoms(
      conjunctiveQuery,
      (atom: Atom) => {
        // variables in the atom that are bound in the CQ
        val atomBoundVariables = SetLikeExtensions.intersection(
          atom.getVariables.toList.asJavaCollection,
          conjunctiveQuery.getBoundVariables.toSet.asJavaCollection
        )
        variableSet.containsAll(atomBoundVariables) && atomBoundVariables.size > 0
      }
    )
  }

  /**
   * Computes a subquery of a given Conjunctive Query that includes only the atoms which have at
   * least one bound variable in a specified set of variables. <p> For example, if {@code
   * conjunctiveQuery} is {@code ∃x,y,z. T(x,y,w) ∧ T(x,c,z)} and {@code boundVariableSet} is
   * {@code {y}}, then the returned subquery is {@code ∃x,y. T(x,y,w)}. <p> If no atom in the
   * given {@code ConjunctiveQuery} has variable set intersecting with {@code variables}, an
   * empty {@code Optional} is returned.
   */
  def subqueryRelevantToVariables(conjunctiveQuery: ConjunctiveQuery,
                                  variables: util.Collection[_ <: Variable]
  ): Optional[ConjunctiveQuery] = {
    val variableSet = ImmutableSet.copyOf[Variable](variables)

    filterAtoms(
      conjunctiveQuery,
      (atom: Atom) => {
        // variables in the atom that are bound in the CQ
        val atomBoundVariables = SetLikeExtensions.intersection(
          atom.getVariables.toList.asJavaCollection,
          conjunctiveQuery.getBoundVariables.toList.asJavaCollection
        )
        SetLikeExtensions.nontriviallyIntersects(atomBoundVariables, variableSet)
      }
    )
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
    variables: util.Collection[_ <: Variable]
  ): ImmutableSet[Variable] = {
    val subquery = subqueryRelevantToVariables(conjunctiveQuery, variables)
    if (subquery.isEmpty) return ImmutableSet.of
    SetLikeExtensions.difference(variablesIn(subquery.get), variables)
  }

  /**
   * Given a conjunctive query {@code conjunctiveQuery} and a set {@code boundVariables} of
   * variables bound in {@code conjunctiveQuery}, returns a stream of all {@code
   * conjunctiveQuery}-connected components of {@code variables}.
   */
  def connectedComponents(conjunctiveQuery: ConjunctiveQuery,
                          boundVariables: util.Collection[_ <: Variable]
  ): Stream[ImmutableSet[Variable]] = {
    if (boundVariables.isEmpty) return Stream.empty

    import scala.jdk.CollectionConverters._
    for (variable <- boundVariables.asScala) {
      if (!conjunctiveQuery.getBoundVariables.contains(variable))
        throw new IllegalArgumentException(
          "Variable " + variable + " is not bound in the given CQ"
        )
    }

    val unionFindTree = new SimpleUnionFindTree[Variable](boundVariables)
    for (atom <- conjunctiveQuery.getAtoms) {
      val variablesToUnion =
        SetLikeExtensions.intersection(ImmutableSet.copyOf(atom.getVariables), boundVariables)
      unionFindTree.unionAll(variablesToUnion)
    }
    unionFindTree.getEquivalenceClasses.stream
  }

  /**
   * Given a conjunctive query {@code q} and a set {@code v} of variables in {@code q}, checks
   * if {@code v} is connected in {@code q}. <p> A set of variables {@code V} is said to be
   * connected in {@code q} if there is at most one {@code q}-connected component of {@code V}
   * in {@code q}.
   */
  def isConnected(
    conjunctiveQuery: ConjunctiveQuery,
    variables: util.Collection[_ <: Variable]
  ): Boolean = connectedComponents(conjunctiveQuery, variables).count <= 1L

  def constantsIn(conjunctiveQuery: ConjunctiveQuery): ImmutableSet[Constant] =
    ImmutableSet.copyOf(StreamExtensions.filterSubtype(
      util.Arrays.stream(conjunctiveQuery.getTerms),
      classOf[Constant]
    ).iterator)

  def variablesIn(conjunctiveQuery: ConjunctiveQuery): ImmutableSet[Variable] =
    SetLikeExtensions.union(
      util.Arrays.asList(conjunctiveQuery.getBoundVariables: _*),
      util.Arrays.asList(conjunctiveQuery.getFreeVariables: _*)
    )
}
