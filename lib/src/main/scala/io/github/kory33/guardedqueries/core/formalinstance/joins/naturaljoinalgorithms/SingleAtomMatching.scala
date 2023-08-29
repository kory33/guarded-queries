package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import java.util
import java.util.Optional

object SingleAtomMatching {
  private def tryMatch[TA](atomicQuery: Atom,
                           orderedQueryVariables: List[Variable],
                           appliedTerms: List[TA],
                           includeConstantsToTA: Constant => TA
  ): Optional[List[TA]] = {
    val homomorphism = new util.ArrayList[Optional[TA]](orderedQueryVariables.size)

    for (i <- 0 until orderedQueryVariables.size) { homomorphism.add(Optional.empty) }

    for (appliedTermIndex <- 0 until appliedTerms.size) {
      val termToMatch = atomicQuery.getTerms()(appliedTermIndex)
      val appliedTerm = appliedTerms.get(appliedTermIndex)

      termToMatch match {
        case constant: Constant =>
          // if the term is a constant, we just check if that constant (considered as TA) has been applied
          if (!(includeConstantsToTA.apply(constant) == appliedTerm)) {
            // and fail if not
            return Optional.empty
          }
        case variable: Variable =>
          val variableIndex = orderedQueryVariables.indexOf(termToMatch)
          val alreadyAssignedConstant = homomorphism.get(variableIndex)
          if (alreadyAssignedConstant.isPresent) {
            // if the variable has already been assigned a constant, we check if the constant is the same
            if (!(alreadyAssignedConstant.get == appliedTerm)) {
              // and fail if not
              return Optional.empty
            }
          } else {
            // if the variable has not already been assigned a constant, we assign it
            homomorphism.set(variableIndex, Optional.of(appliedTerm))
          }
        case _ =>
      }
    }

    // if we have reached this point, we have successfully matched all variables in the query
    // to constants applied to the fact, so return the homomorphism
    val unwrappedHomomorphism =
      List.copyOf(homomorphism.stream.map(_.get).iterator)
    Optional.of(unwrappedHomomorphism)
  }

  /**
   * Finds all answers to the given atomic query in the given instance. <p> The returned join
   * result is well-formed.
   *
   * @throws IllegalArgumentException
   *   if the given query contains a term that is neither a variable nor a constant
   */
  def allMatches[TA](atomicQuery: Atom,
                     instance: FormalInstance[TA],
                     includeConstantsToTA: Constant => TA
  ): JoinResult[TA] = {
    val orderedQueryVariables =
      List.copyOf(Set.copyOf(atomicQuery.getVariables))
    val queryPredicate = atomicQuery.getPredicate
    val homomorphisms = List.builder[List[TA]]

    import scala.jdk.CollectionConverters._
    for (fact <- instance.facts.asScala) {
      if (fact.predicate == queryPredicate) {
        // compute a homomorphism and add to the builder, or continue to the next fact if we cannot do so
        tryMatch(
          atomicQuery,
          orderedQueryVariables,
          fact.appliedTerms,
          includeConstantsToTA
        ).ifPresent(homomorphisms.add)
      }
    }

    new JoinResult[TA](orderedQueryVariables, homomorphisms.build)
  }
}
class SingleAtomMatching private {}
