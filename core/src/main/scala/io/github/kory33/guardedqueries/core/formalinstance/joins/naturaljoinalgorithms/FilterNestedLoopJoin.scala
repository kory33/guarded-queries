package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.QueryLikeAtom
import io.github.kory33.guardedqueries.core.formalinstance.QueryLikeInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import uk.ac.ox.cs.pdq.fol.*

import scala.collection.mutable.ArrayBuffer
import scala.util.boundary

/**
 * A join algorithm that first filters tuples matching query atoms and then joins them in a
 * nested loop fashion.
 */
class FilterNestedLoopJoin[QueryVariable, Constant]
    extends NaturalJoinAlgorithm[QueryVariable, Constant, FormalInstance[Constant]] {

  private type QueryAtom = QueryLikeAtom[QueryVariable, Constant]
  private type Query = QueryLikeInstance[QueryVariable, Constant]
  private type Instance = FormalInstance[Constant]

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
  private def visitAllJoinResults(
    remainingAtomsToJoin: List[QueryAtom],
    atomToMatches: Map[QueryAtom, JoinResult[QueryVariable, Constant]],
    resultVariableOrdering: List[QueryVariable],
    partialHomomorphism: List[Option[Constant]],
    visitEachCompleteHomomorphism: List[Constant] => Unit
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

  override def join(query: Query,
                    formalInstance: Instance
  ): JoinResult[QueryVariable, Constant] = {
    val relevantRelationsToInstancesMap: Map[Predicate, Instance] = {
      formalInstance.facts
        .filter(fact => query.activePredicates.contains(fact.predicate))
        .groupBy(_.predicate)
        .view.mapValues(FormalInstance(_)).toMap
    }

    val queryAtomsToMatches: Map[QueryAtom, JoinResult[QueryVariable, Constant]] =
      query.facts.associate { atom =>
        val relevantInstance =
          relevantRelationsToInstancesMap.getOrElse(atom.predicate, FormalInstance.empty)

        SingleAtomMatching.allMatches(atom, relevantInstance)
      }

    // we start joining from the outer
    val queryAtomsOrderedByMatchSizes: List[QueryAtom] =
      queryAtomsToMatches.keySet.toList.sortBy(atom =>
        queryAtomsToMatches(atom).allHomomorphisms.size
      )

    val queryVariableOrdering: List[QueryVariable] = query.facts
      .flatMap(_.appliedTerms)
      .collect { case Left(variable) => variable }
      .toList
    val emptyHomomorphism = queryVariableOrdering.map(_ => None)
    val resultBuilder = ArrayBuffer[List[Constant]]()

    visitAllJoinResults(
      queryAtomsOrderedByMatchSizes,
      queryAtomsToMatches,
      queryVariableOrdering,
      emptyHomomorphism,
      resultBuilder.append
    )

    // FIXME: It benefits us (especially with nonEmpty calls) to use a lazy collection here.
    JoinResult[QueryVariable, Constant](queryVariableOrdering, resultBuilder.toList)
  }
}
