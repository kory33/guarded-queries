package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance, IncludesFolConstants}
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import uk.ac.ox.cs.pdq.fol.{Atom, Constant, Variable}

import scala.collection.mutable
import scala.util.boundary

object SingleAtomMatching {
  private def tryMatch[TA: IncludesFolConstants](
    atomicQuery: Atom,
    orderedQueryVariables: List[Variable],
    appliedTerms: List[TA]
  ): Option[List[TA]] = boundary { returnMethod ?=>
    val homomorphismMap = mutable.HashMap.empty[Variable, TA]

    for ((termToMatch, appliedTerm) <- atomicQuery.getTerms.zip(appliedTerms)) {
      termToMatch match {
        case constant: Constant =>
          // if the term is a constant, we only need to verify that the constant
          // is the same as appliedTerm at the same argument position
          if (IncludesFolConstants[TA].includeConstant(constant) != appliedTerm) {
            boundary.break(None)(using returnMethod)
          }
        case variable: Variable =>
          homomorphismMap.get(variable) match {
            case None =>
              // if the variable has not already been assigned a term, we assign appliedTerm
              homomorphismMap(variable) = appliedTerm
            case Some(alreadyAssignedTerm) if alreadyAssignedTerm != appliedTerm =>
              // the variable has been assigned a term, but termToMatch conflicts with the old assignment
              boundary.break(None)(using returnMethod)
            case _ => ()
          }
        case _ =>
          throw IllegalArgumentException(
            s"term $termToMatch in query $atomicQuery is neither a variable nor a constant"
          )
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
  def allMatches[TA: IncludesFolConstants](
    atomicQuery: Atom,
    instance: FormalInstance[TA]
  ): JoinResult[TA] = {
    val orderedQueryVariables: List[Variable] = atomicQuery.getVariables.toSet.toList
    val relevantFacts: Set[FormalFact[TA]] =
      instance.facts.filter(_.predicate == atomicQuery.getPredicate)

    val homomorphisms = relevantFacts.flatMap { fact =>
      tryMatch(atomicQuery, orderedQueryVariables, fact.appliedTerms)
    }

    JoinResult[TA](orderedQueryVariables, homomorphisms.toList)
  }
}
