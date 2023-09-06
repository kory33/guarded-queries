package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.{
  FormalFact,
  FormalInstance,
  QueryLikeAtom
}
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult

import scala.collection.mutable
import scala.util.boundary

object SingleAtomMatching {
  private def tryMatch[QueryVariable, Constant](
    atomicQuery: QueryLikeAtom[QueryVariable, Constant],
    orderedQueryVariables: List[QueryVariable],
    appliedTerms: List[Constant]
  ): Option[List[Constant]] = boundary { returnMethod ?=>
    val homomorphismMap = mutable.HashMap.empty[QueryVariable, Constant]

    for ((termToMatch, appliedTerm) <- atomicQuery.appliedTerms.zip(appliedTerms)) {
      termToMatch match {
        case Right(constant) =>
          // if the term is a constant, we only need to verify that the constant
          // is the same as appliedTerm at the same argument position
          if (constant != appliedTerm) {
            boundary.break(None)(using returnMethod)
          }
        case Left(variable) =>
          homomorphismMap.get(variable) match {
            case None =>
              // if the variable has not already been assigned a term, we assign appliedTerm
              homomorphismMap(variable) = appliedTerm
            case Some(alreadyAssignedTerm) if alreadyAssignedTerm != appliedTerm =>
              // the variable has been assigned a term, but termToMatch conflicts with the old assignment
              boundary.break(None)(using returnMethod)
            case _ => ()
          }
      }
    }

    // if we have reached this point, we have successfully matched all variables in the query
    // to terms applied in the fact, so construct a homomorphism and return it
    Some(orderedQueryVariables.map(homomorphismMap))
  }

  /**
   * Finds all answers to the given atomic query in the given instance.
   *
   * The returned join result is well-formed.
   *
   * @throws IllegalArgumentException
   *   if the given query contains a term that is neither a variable nor a constant
   */
  def allMatches[QueryVariable, Constant](
    atomicQuery: QueryLikeAtom[QueryVariable, Constant],
    instance: FormalInstance[Constant]
  ): JoinResult[QueryVariable, Constant] = {
    val orderedQueryVariables: List[QueryVariable] = atomicQuery
      .appliedTerms
      .collect { case Left(variable) => variable }
      .distinct

    val relevantFacts: Set[FormalFact[Constant]] =
      instance.facts.filter(_.predicate == atomicQuery.predicate)

    val homomorphisms = relevantFacts.flatMap { fact =>
      tryMatch(atomicQuery, orderedQueryVariables, fact.appliedTerms)
    }

    JoinResult[QueryVariable, Constant](orderedQueryVariables, homomorphisms.toList)
  }
}
