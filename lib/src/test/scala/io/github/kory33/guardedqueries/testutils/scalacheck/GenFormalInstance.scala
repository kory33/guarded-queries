package io.github.kory33.guardedqueries.testutils.scalacheck

import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.ShrinkSet
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.TraverseListGen
import org.scalacheck.Gen
import org.scalacheck.Shrink
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate

object GenFormalInstance {
  def genFormalInstanceOver(predicate: Predicate,
                            constantsToUse: Set[Constant]
  ): Gen[FormalInstance[Constant]] = {
    def buildTuples(currentTuple: List[Constant] = Nil): List[List[Constant]] = {
      if (currentTuple.size == predicate.getArity) {
        List(currentTuple)
      } else {
        constantsToUse.toList.flatMap { constant => buildTuples(constant :: currentTuple) }
      }
    }

    for {
      tupleSet <- GenSet.chooseSubset(buildTuples(Nil).toSet)
      factSet = tupleSet.map(FormalFact(predicate, _))
    } yield FormalInstance(factSet)
  }

  def genFormalInstanceContainingPredicates(predicates: Set[Predicate],
                                            constantsToUse: Set[Constant]
  ): Gen[FormalInstance[Constant]] = {
    import TraverseListGen.traverse
    predicates.toList
      .traverse(predicate => genFormalInstanceOver(predicate, constantsToUse))
      .map { instanceList => FormalInstance(instanceList.flatMap(_.facts).toSet) }
  }
}

object ShrinkFormalInstance {
  given Shrink[FormalInstance[Constant]] = Shrink { instance =>
    ShrinkSet.intoSubsets[FormalFact[Constant]]
      .shrink(instance.facts)
      .map(FormalInstance(_))
  }
}
