package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormalInstance
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormula
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.TraverseListGen.traverse
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.TypedConstant
import uk.ac.ox.cs.pdq.fol.Variable

import scala.jdk.CollectionConverters._

class FilterNestedLoopJoinSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  def smallNonExistentialQueryOver(predicateSet: Set[Predicate]): Gen[ConjunctiveQuery] = for {
    querySize <- Gen.chooseNum(1, 5)
    queryAtoms <- Gen.listOfN(
      querySize,
      Gen.oneOf(predicateSet).flatMap { predicate =>
        val genTerm = Gen.oneOf(
          GenFormula.genNumberedVariable(5),
          GenFormula.genConstant(10)
        )
        Gen.listOfN(predicate.getArity, genTerm).map { terms => Atom.create(predicate, terms*) }
      }
    )
    queryVariables = queryAtoms.flatMap(_.getVariables()).toSet
  } yield ConjunctiveQuery.create(queryVariables.toArray, queryAtoms.toArray)

  val genInstanceAndJoinQuery: Gen[(FormalInstance[Constant], ConjunctiveQuery)] = for {
    predicateSet <- GenFormalInstance.genSmallPredicateSet
    constantsToUse = (1 to 10).map(i => TypedConstant.create(s"c_$i"): Constant).toSet
    instance <-
      GenFormalInstance.genFormalInstanceContainingPredicates(predicateSet, constantsToUse)
    query <- smallNonExistentialQueryOver(predicateSet)
  } yield (instance, query)

  "All homomorphisms in FilterNestedLoopJoin.join" should "materialize a subset of the input instance" in {
    forAll(genInstanceAndJoinQuery, minSuccessful(100)) {
      case (instance, query) =>
        new FilterNestedLoopJoin().joinConjunctiveQuery(query, instance)
          .allHomomorphisms
          .foreach { homomorphism =>
            val materializedInstance =
              homomorphism.materializeFunctionFreeAtoms(query.getAtoms.toSet)
            assert(instance.isSuperInstanceOf(materializedInstance))
          }
    }
  }

  val genQueryAndHomomorphism: Gen[(ConjunctiveQuery, HomomorphicMapping[Variable, Constant])] =
    for {
      predicateSet <- GenFormalInstance.genSmallPredicateSet
      query <- smallNonExistentialQueryOver(predicateSet)
      variablesInQuery = query.getFreeVariables.toSet
      homomorphism <-
        Gen.listOfN(variablesInQuery.size, GenFormula.genConstant(15)).map { constants =>
          HomomorphicMapping(variablesInQuery.toList, constants)
        }
    } yield (query, homomorphism)

  def equivalentAsHomomorphisms(h1: HomomorphicMapping[Variable, Constant],
                                h2: HomomorphicMapping[Variable, Constant]
  ): Boolean = h1.toMap == h2.toMap

  "FilterNestedLoopJoin.join" should "find every valid answer" in {
    forAll(genQueryAndHomomorphism, minSuccessful(3000)) {
      case (query, originalHomomorphism) =>
        val materializedInstance = originalHomomorphism
          .materializeFunctionFreeAtoms(query.getAtoms.toSet)

        assert {
          new FilterNestedLoopJoin[Variable, Constant]
            .joinConjunctiveQuery(query, materializedInstance)
            .allHomomorphisms
            .exists(equivalentAsHomomorphisms(_, originalHomomorphism))
        }
    }
  }
}
