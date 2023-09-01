package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.IncludesFolConstants
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import uk.ac.ox.cs.pdq.fol.*

import scala.collection.mutable.ArrayBuffer
import scala.util.boundary

/**
 * A join algorithm that first filters tuples matching query atoms and then joins them in a
 * nested loop fashion.
 */
class FilterNestedLoopJoin[TA: IncludesFolConstants]
    extends NaturalJoinAlgorithm[TA, FormalInstance[TA]] {
  override def join(query: ConjunctiveQuery,
                    formalInstance: FormalInstance[TA]
  ): JoinResult[TA] = {
    // we throw IllegalArgumentException if the query contains existential atoms
    if (query.getBoundVariables.length != 0)
      throw new IllegalArgumentException("NestedLoopJoin does not support existential queries")

    val relevantRelationsToInstancesMap: Map[Predicate, FormalInstance[TA]] = {
      val queryPredicates = query.allPredicates
      formalInstance.facts
        .filter(fact => queryPredicates.contains(fact.predicate))
        .groupBy(_.predicate)
        .view.mapValues(FormalInstance(_)).toMap
    }

    val queryAtomsToMatches: Map[Atom, JoinResult[TA]] = query.getAtoms.toVector
      .associate { atom =>
        val relevantInstance: FormalInstance[TA] = relevantRelationsToInstancesMap
          .getOrElse(atom.getPredicate, FormalInstance.empty)

        SingleAtomMatching.allMatches(atom, relevantInstance)
      }.toMap

    // we start joining from the outer
    val queryAtomsOrderedByMatchSizes: List[Atom] =
      queryAtomsToMatches.keySet.toList.sortBy(atom =>
        queryAtomsToMatches(atom).allHomomorphisms.size
      )

    val queryVariableOrdering: List[Variable] = query.allVariables.toList
    val emptyHomomorphism = queryVariableOrdering.map(_ => None)
    val resultBuilder = ArrayBuffer[List[TA]]()

    FilterNestedLoopJoin.visitAllJoinResults(
      queryAtomsOrderedByMatchSizes,
      queryAtomsToMatches,
      queryVariableOrdering,
      emptyHomomorphism,
      resultBuilder.append
    )

    JoinResult[TA](queryVariableOrdering, resultBuilder.toList)
  }
}

object FilterNestedLoopJoin {

  /**
   * Recursively nest matching loops to extend the given partial homomorphism, in the order
   * specified by `remainingAtomsToJoin`.
   *
   * Each result of the successful join is passed to `visitEachCompleteHomomorphism`.
   *
   * As a successful join uniquely determines the sequence of tuples in join results
   * corresponding to the sequence of query atoms, as long as each `JoinResult` in
   * `atomToMatches` does not contain duplicate tuples, no duplicate tuples will be passed to
   * `visitEachCompleteHomomorphism`.
   */
  private def visitAllJoinResults[TA](remainingAtomsToJoin: List[Atom],
                                      atomToMatches: Map[Atom, JoinResult[TA]],
                                      resultVariableOrdering: List[Variable],
                                      partialHomomorphism: List[Option[TA]],
                                      visitEachCompleteHomomorphism: List[TA] => Unit
  ): Unit = remainingAtomsToJoin match {
    case Nil =>
      // If there are no more atoms to join, the given partial homomorphism
      // should be complete. So unwrap Option and visit
      val unwrappedHomomorphism = partialHomomorphism.map(_.get)
      visitEachCompleteHomomorphism(unwrappedHomomorphism)

    case nextAtomToMatch :: nextRemainingAtomsToJoin =>
      for (atomMatch <- atomToMatches(nextAtomToMatch).allHomomorphisms) {
        boundary { continueMatchLoop ?=>
          val homomorphismExtension = partialHomomorphism.toArray

          // try to extend partialHomomorphism with atomMatch, but break to the boundary
          // (i.e. continue the outer loop) if atomMatch conflicts with partialHomomorphism
          for ((nextVariableToCheck, extensionCandidate) <- atomMatch.toMap) {
            val indexOfVariableInResult = resultVariableOrdering.indexOf(nextVariableToCheck)

            homomorphismExtension(indexOfVariableInResult) match
              case None =>
                // no term is assigned to the variable yet,
                // so we are free to extend the homomorphism to the variable
                homomorphismExtension(indexOfVariableInResult) = Some(extensionCandidate)
              case Some(existingResult) if existingResult != extensionCandidate =>
                // the match cannot extend the partialHomomorphism, so skip to the next match
                boundary.break()(using continueMatchLoop)
              case _ => ()
          }

          // at this point the match must have been extended to cover all variables in the atom,
          // so proceed
          visitAllJoinResults(
            nextRemainingAtomsToJoin,
            atomToMatches,
            resultVariableOrdering,
            homomorphismExtension.toList,
            visitEachCompleteHomomorphism
          )
        }
      }
  }
}
