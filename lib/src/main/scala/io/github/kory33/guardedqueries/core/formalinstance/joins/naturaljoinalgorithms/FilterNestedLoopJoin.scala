package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.utils.extensions.ImmutableMapExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions
import uk.ac.ox.cs.pdq.fol._
import java.util
import java.util.Comparator
import java.util.Optional
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * A join algorithm that first filters tuples matching query atoms and then joins them in a
 * nested loop fashion.
 */
object FilterNestedLoopJoin {

  /**
   * Recursively nest matching loops to extend the given partial homomorphism, in the order
   * specified by {@code remainingAtomsToJoin}. <p> Each result of the successful join is passed
   * to {@code visitEachCompleteHomomorphism}. <p> As a successful join uniquely determines the
   * sequence of tuples in join results corresponding to the sequence of query atoms, as long as
   * each {@code JoinResult} in {@code atomToMatches} does not contain duplicate tuples, no
   * duplicate tuples will be passed to {@code visitEachCompleteHomomorphism}.
   */
  private def visitAllJoinResults[TA](remainingAtomsToJoin: ImmutableList[Atom],
                                      atomToMatches: ImmutableMap[Atom, JoinResult[TA]],
                                      resultVariableOrdering: ImmutableList[Variable],
                                      partialHomomorphism: ImmutableList[Optional[TA]],
                                      visitEachCompleteHomomorphism: Consumer[ImmutableList[TA]]
  ): Unit = {
    if (remainingAtomsToJoin.isEmpty) {
      // If there are no more atoms to join, the given partial homomorphism
      // should be complete. So unwrap Optionals and visit
      val unwrappedHomomorphism =
        ImmutableList.copyOf[TA](partialHomomorphism.stream.map(_.get).iterator)

      visitEachCompleteHomomorphism.accept(unwrappedHomomorphism)
      return
    }

    val nextAtomMatchResult = atomToMatches.get(remainingAtomsToJoin.get(0))

    import scala.jdk.CollectionConverters._
    import scala.util.boundary
    for (homomorphism <- nextAtomMatchResult.allHomomorphisms.asScala) {
      boundary {
        val matchVariableOrdering = homomorphism.variableOrdering
        val homomorphismExtension = new util.ArrayList[Optional[TA]](partialHomomorphism)

        for (matchVariableIndex <- 0 until matchVariableOrdering.size) {
          val nextVariableToCheck = matchVariableOrdering.get(matchVariableIndex)
          val extensionCandidate = homomorphism.orderedMapping.get(matchVariableIndex)
          val indexOfVariableInResultOrdering =
            resultVariableOrdering.indexOf(nextVariableToCheck)
          val mappingSoFar = homomorphismExtension.get(indexOfVariableInResultOrdering)
          if (mappingSoFar.isEmpty) {
            // we are free to extend the homomorphism to the variable
            homomorphismExtension.set(
              indexOfVariableInResultOrdering,
              Optional.of(extensionCandidate)
            )
          } else if (!(mappingSoFar.get == extensionCandidate)) {
            // the match cannot extend the partialHomomorphism, so skip to the next match
            boundary.break()
          }
        }

        // at this point the match must have been extended to cover all variables in the atom,
        // so proceed
        visitAllJoinResults(
          remainingAtomsToJoin.subList(1, remainingAtomsToJoin.size),
          atomToMatches,
          resultVariableOrdering,
          ImmutableList.copyOf(homomorphismExtension),
          visitEachCompleteHomomorphism
        )
      }
    }
  }
}
class FilterNestedLoopJoin[TA](private val includeConstantsToTA: Function[Constant, TA])
    extends NaturalJoinAlgorithm[TA, FormalInstance[TA]] {
  override def join(query: ConjunctiveQuery,
                    formalInstance: FormalInstance[TA]
  ): JoinResult[TA] = {
    val queryAtoms = ImmutableSet.copyOf(query.getAtoms)

    // we throw IllegalArgumentException if the query contains existential atoms
    if (query.getBoundVariables.length != 0)
      throw new IllegalArgumentException("NestedLoopJoin does not support existential queries")

    val queryPredicates =
      util.Arrays.stream(query.getAtoms).map(_.getPredicate).collect(Collectors.toSet)

    val relevantRelationsToInstancesMap: ImmutableMap[Predicate, FormalInstance[TA]] = {
      val relevantFactsStream = formalInstance.facts.stream.filter((fact: FormalFact[TA]) =>
        queryPredicates.contains(fact.predicate)
      )

      val groupedFacts =
        relevantFactsStream.collect(Collectors.groupingBy[FormalFact[TA], Predicate](_.predicate))

      MapExtensions.composeWithFunction(groupedFacts, FormalInstance(_))
    }

    val queryAtomsToMatches = ImmutableMapExtensions.consumeAndCopy(StreamExtensions.associate(
      queryAtoms.stream,
      (atom: Atom) =>
        SingleAtomMatching.allMatches(
          atom,
          relevantRelationsToInstancesMap.getOrDefault(atom.getPredicate, FormalInstance.empty),
          includeConstantsToTA
        )
    ).iterator)

    // we start joining from the outer
    val queryAtomsOrderedByMatchSizes =
      ImmutableList.copyOf(queryAtomsToMatches.keySet.stream.sorted(Comparator.comparing(
        (atom: Atom) => queryAtomsToMatches.get(atom).allHomomorphisms.size
      )).iterator)

    val queryVariableOrdering =
      ImmutableList.copyOf(ImmutableSet.copyOf(util.Arrays.stream(query.getAtoms).flatMap(
        (atom: Atom) => util.Arrays.stream(atom.getVariables)
      ).iterator))

    val emptyHomomorphism =
      ImmutableList.copyOf(queryVariableOrdering.stream.map((v: Variable) =>
        Optional.empty[TA]
      ).iterator)

    val resultBuilder = ImmutableList.builder[ImmutableList[TA]]

    FilterNestedLoopJoin.visitAllJoinResults(
      queryAtomsOrderedByMatchSizes,
      queryAtomsToMatches,
      queryVariableOrdering,
      emptyHomomorphism,
      resultBuilder.add
    )

    new JoinResult[TA](queryVariableOrdering, resultBuilder.build)
  }
}
