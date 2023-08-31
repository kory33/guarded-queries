package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.{FormalInstance, IncludesFolConstants}
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import uk.ac.ox.cs.pdq.fol.{Atom, Constant, Variable}

import scala.util.boundary
import scala.collection.mutable.ArrayBuffer

object SingleAtomMatching {
  private def tryMatch[TA: IncludesFolConstants](
    atomicQuery: Atom,
    orderedQueryVariables: List[Variable],
    appliedTerms: List[TA]
  ): Option[List[TA]] = boundary {
    val homomorphism = ArrayBuffer.fill[Option[TA]](orderedQueryVariables.size)(None)

    for (appliedTermIndex <- appliedTerms.indices) {
      val termToMatch = atomicQuery.getTerms()(appliedTermIndex)
      val appliedTerm = appliedTerms(appliedTermIndex)

      termToMatch match {
        case constant: Constant =>
          // if the term is a constant, we just check if that constant (considered as TA) has been applied
          if (!(IncludesFolConstants[TA].includeConstant(constant) == appliedTerm)) {
            // and fail if not
            boundary.break(None)
          }
        case variable: Variable =>
          val variableIndex = orderedQueryVariables.indexOf(termToMatch)
          val alreadyAssignedConstant = homomorphism(variableIndex)
          if (alreadyAssignedConstant.isDefined) {
            // if the variable has already been assigned a constant, we check if the constant is the same
            if (!(alreadyAssignedConstant.get == appliedTerm)) {
              // and fail if not
              boundary.break(None)
            }
          } else {
            // if the variable has not already been assigned a constant, we assign it
            homomorphism(variableIndex) = Some(appliedTerm)
          }
        case _ =>
      }
    }

    // if we have reached this point, we have successfully matched all variables in the query
    // to constants applied to the fact, so return the homomorphism
    Some(homomorphism.map(_.get).toList)
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
    val orderedQueryVariables = atomicQuery.getVariables.toSet.toList
    val queryPredicate = atomicQuery.getPredicate
    val homomorphisms = ArrayBuffer.empty[List[TA]]

    import scala.jdk.CollectionConverters.*
    for (fact <- instance.facts) {
      if (fact.predicate == queryPredicate) {
        // compute a homomorphism and add to the builder, or continue to the next fact if we cannot do so
        tryMatch(
          atomicQuery,
          orderedQueryVariables,
          fact.appliedTerms
        ).foreach(homomorphisms.append)
      }
    }

    JoinResult[TA](orderedQueryVariables, homomorphisms.toList)
  }
}
