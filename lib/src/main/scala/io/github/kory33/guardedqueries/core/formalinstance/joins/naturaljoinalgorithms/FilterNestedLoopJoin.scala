package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.joins.{
  JoinResult,
  NaturalJoinAlgorithm
}
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import uk.ac.ox.cs.pdq.fol.*

import scala.collection.mutable.ArrayBuffer

/**
 * A join algorithm that first filters tuples matching query atoms and then joins them in a
 * nested loop fashion.
 */
object FilterNestedLoopJoin {

  /**
   * Recursively nest matching loops to extend the given partial homomorphism, in the order
   * specified by {@code remainingAtomsToJoin}.
   *
   * Each result of the successful join is passed to {@code visitEachCompleteHomomorphism}.
   *
   * As a successful join uniquely determines the sequence of tuples in join results
   * corresponding to the sequence of query atoms, as long as each {@code JoinResult} in {@code
   * atomToMatches} does not contain duplicate tuples, no duplicate tuples will be passed to
   * {@code visitEachCompleteHomomorphism}.
   */
  private def visitAllJoinResults[TA](remainingAtomsToJoin: List[Atom],
                                      atomToMatches: Map[Atom, JoinResult[TA]],
                                      resultVariableOrdering: List[Variable],
                                      partialHomomorphism: List[Option[TA]],
                                      visitEachCompleteHomomorphism: List[TA] => Unit
  ): Unit = {
    if (remainingAtomsToJoin.isEmpty) {
      // If there are no more atoms to join, the given partial homomorphism
      // should be complete. So unwrap Option and visit
      val unwrappedHomomorphism = partialHomomorphism.map(_.get)
      visitEachCompleteHomomorphism(unwrappedHomomorphism)
      return
    }

    val nextAtomMatchResult = atomToMatches(remainingAtomsToJoin(0))

    import scala.util.boundary
    for (homomorphism <- nextAtomMatchResult.allHomomorphisms) {
      boundary {
        val matchVariableOrdering = homomorphism.variableOrdering
        val homomorphismExtension = partialHomomorphism.toArray

        for (matchVariableIndex <- 0 until matchVariableOrdering.size) {
          val nextVariableToCheck = matchVariableOrdering(matchVariableIndex)
          val extensionCandidate = homomorphism.orderedMapping(matchVariableIndex)
          val indexOfVariableInResultOrdering =
            resultVariableOrdering.indexOf(nextVariableToCheck)

          val mappingSoFar = homomorphismExtension(indexOfVariableInResultOrdering)
          if (mappingSoFar.isEmpty) {
            // we are free to extend the homomorphism to the variable
            homomorphismExtension(indexOfVariableInResultOrdering) = Some(extensionCandidate)
          } else if (!(mappingSoFar.get == extensionCandidate)) {
            // the match cannot extend the partialHomomorphism, so skip to the next match
            boundary.break()
          }
        }

        // at this point the match must have been extended to cover all variables in the atom,
        // so proceed
        visitAllJoinResults(
          remainingAtomsToJoin.tail,
          atomToMatches,
          resultVariableOrdering,
          homomorphismExtension.toList,
          visitEachCompleteHomomorphism
        )
      }
    }
  }
}
class FilterNestedLoopJoin[TA](private val includeConstantsToTA: Constant => TA)
    extends NaturalJoinAlgorithm[TA, FormalInstance[TA]] {
  override def join(query: ConjunctiveQuery,
                    formalInstance: FormalInstance[TA]
  ): JoinResult[TA] = {
    val queryAtoms = query.getAtoms.toSet

    // we throw IllegalArgumentException if the query contains existential atoms
    if (query.getBoundVariables.length != 0)
      throw new IllegalArgumentException("NestedLoopJoin does not support existential queries")

    val queryPredicates = query.getAtoms.map(_.getPredicate).toSet

    val relevantRelationsToInstancesMap: Map[Predicate, FormalInstance[TA]] =
      formalInstance.facts
        .filter(fact => queryPredicates.contains(fact.predicate))
        .groupBy(_.predicate)
        .view.mapValues(FormalInstance(_)).toMap

    val queryAtomsToMatches = queryAtoms.map { atom =>
      atom -> SingleAtomMatching.allMatches(
        atom,
        relevantRelationsToInstancesMap.getOrElse(atom.getPredicate, FormalInstance.empty),
        includeConstantsToTA
      )
    }.toMap

    // we start joining from the outer
    val queryAtomsOrderedByMatchSizes =
      queryAtomsToMatches.keySet.toList.sortBy(atom =>
        queryAtomsToMatches(atom).allHomomorphisms.size
      )

    val queryVariableOrdering =
      query.getAtoms.flatMap((atom: Atom) => atom.getVariables).toSet.toList

    val emptyHomomorphism = queryVariableOrdering.map(_ => None)

    val resultBuilder = ArrayBuffer[List[TA]]()

    FilterNestedLoopJoin.visitAllJoinResults(
      queryAtomsOrderedByMatchSizes,
      queryAtomsToMatches,
      queryVariableOrdering,
      emptyHomomorphism,
      resultBuilder.append
    )

    new JoinResult[TA](queryVariableOrdering, resultBuilder.toList)
  }
}
