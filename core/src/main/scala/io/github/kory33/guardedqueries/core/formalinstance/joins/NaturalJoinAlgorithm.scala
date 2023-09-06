package io.github.kory33.guardedqueries.core.formalinstance.joins

import io.github.kory33.guardedqueries.core.formalinstance.*
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Variable}

/**
 * An interface to objects that can answer natural join queries over database instances.
 *
 * Implementations of this interface need to support answering conjunctive queries that <ol>
 * <li>does not contain existential variables</li> <li>contains only variables and constants as
 * terms</li> </ol>
 */
trait NaturalJoinAlgorithm[QueryVariable, Constant, Instance] {

  /**
   * Finds all answers to the given conjunctive query in the given instance.
   *
   * @param nonExistentialQuery
   *   a formal instance representing an existential-free conjunctive query
   */
  def join(
    nonExistentialQuery: QueryLikeInstance[QueryVariable, Constant],
    instance: Instance
  ): JoinResult[QueryVariable, Constant]

  /**
   * Finds all answers to the given conjunctive query in the given instance.
   *
   * @param nonExistentialQuery
   *   a conjunctive query containing no existential variables such that every term appearing in
   *   the query is either a variable or a constant
   * @throws IllegalArgumentException
   *   if the given query contains existential variables or contains terms that are neither
   *   variables nor constants
   */
  def joinConjunctiveQuery(nonExistentialQuery: ConjunctiveQuery, instance: Instance)(
    using ev1: QueryVariable =:= Variable,
    ev2: IncludesFolConstants[Constant]
  ): JoinResult[Variable, Constant] =
    import CQAsFormalInstance.given

    ev1.liftCo[NaturalJoinAlgorithm[_, Constant, Instance]](this).join(
      nonExistentialQuery.asQueryLikeFormalInstance,
      instance
    )
}
