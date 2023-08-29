package io.github.kory33.guardedqueries.core.testharnesses

import com.google.common.collect.{List, Map, Set}
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
    val predicateArgIndices =
      List.copyOf(IntStream.range(0, predicate.getArity).iterator)

    val allFormalFacts = MappingStreams.allTotalFunctionsBetween(
      predicateArgIndices,
      constantsToUse.asJava
    ).map((mapping: Map[Integer, Constant]) =>
      new FormalFact[Constant](
        predicate,
        List.copyOf(predicateArgIndices.stream.map(mapping.get).iterator)
      )
    )

    FormalInstance[Constant](allFormalFacts.iterator)
  }

  def randomInstanceOver(signature: FunctionFreeSignature): FormalInstance[Constant] = {
    val constantsToUse =
      (0 until signature.maxArity * 4).map(i => TypedConstant.create(s"c_$i"): Constant).toSet

    val allFactsOverSignature = signature.predicates.flatMap(p =>
      allFactsOver(p, constantsToUse).facts.asScala
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
