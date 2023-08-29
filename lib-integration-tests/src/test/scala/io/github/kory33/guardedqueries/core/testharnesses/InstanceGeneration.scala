package io.github.kory33.guardedqueries.core.testharnesses

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.utils.MappingStreams
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.TypedConstant

import scala.jdk.CollectionConverters._
import java.util.stream.IntStream

object InstanceGeneration {
  def allFactsOver(predicate: Predicate,
                   constantsToUse: Set[Constant]
  ): FormalInstance[Constant] = {
    val predicateArgIndices = (0 until predicate.getArity)

    val allFormalFacts = MappingStreams.allTotalFunctionsBetween(
      predicateArgIndices.toSet,
      constantsToUse
    ).map(mapping =>
      new FormalFact[Constant](
        predicate,
        predicateArgIndices.map(mapping(_)).toList
      )
    )

    FormalInstance[Constant](allFormalFacts.toSet)
  }

  def randomInstanceOver(signature: FunctionFreeSignature): FormalInstance[Constant] = {
    val constantsToUse =
      (0 until signature.maxArity * 4).map(i => TypedConstant.create(s"c_$i"): Constant).toSet

    val allFactsOverSignature = signature.predicates.flatMap(p =>
      allFactsOver(p, constantsToUse).facts
    )

    /**
     * We first decide a selection rate and use it as a threshold to filter out some of the
     * tuples in the instance We are making it more likely to select smaller instances so that
     * the answer set is usually smaller than all of `constantsToUse^(answer arity)`
     */
    val selectionRate = Math.pow(Math.random, 2.5)

    FormalInstance[Constant](allFactsOverSignature.filter((fact: FormalFact[Constant]) =>
      Math.random < selectionRate
    ))
  }
}
