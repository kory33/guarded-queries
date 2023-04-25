package io.github.kory33.guardedqueries.testutils.scalacheck

import scala.jdk.CollectionConverters.*
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Constant
import org.scalacheck.Gen
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.TraverseListGen
import com.google.common.collect.ImmutableList
import uk.ac.ox.cs.pdq.fol.TypedConstant
import org.scalacheck.Shrink
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.ShrinkSet

object GenFormalInstance {
  def genFormalInstanceOver(predicate: Predicate, constantsToUse: Set[Constant]): Gen[FormalInstance[Constant]] = {
    def buildTuples(currentTuple: List[Constant] = Nil): List[List[Constant]] = {
      if (currentTuple.size == predicate.getArity()) {
        List(currentTuple)
      } else {
        constantsToUse.toList.flatMap { constant =>
          buildTuples(constant :: currentTuple)
        }
      }
    }

    for {
      tupleSet <- GenSet.chooseSubset(buildTuples(Nil).toSet)
      factSet = tupleSet
        .map(tuple => ImmutableList.copyOf(tuple.asJavaCollection))
        .map(javaTuple => new FormalFact[Constant](predicate, javaTuple))
    } yield new FormalInstance[Constant](factSet.asJava)
  }

  def genFormalInstanceContainingPredicates(predicates: Set[Predicate]): Gen[FormalInstance[Constant]] = {
    import TraverseListGen.traverse

    val constantsCount = predicates.map(_.getArity()).maxOption.getOrElse(2) * 3
    val constantsToUse = (1 to constantsCount).map(i => TypedConstant.create(s"c_$i"): Constant).toSet

    predicates.toList
      .traverse(predicate => genFormalInstanceOver(predicate, constantsToUse))
      .map { instanceList => new FormalInstance[Constant](instanceList.flatMap(_.facts.asScala).asJava) }
  }
}

object ShrinkFormalInstance {
  given Shrink[FormalInstance[Constant]] = Shrink { instance =>
    ShrinkSet.intoSubsets[FormalFact[Constant]]
      .shrink(instance.facts.asScala.toSet)
      .map(factSet => new FormalInstance[Constant](factSet.asJava))
  }
}
