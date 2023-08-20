package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import io.github.kory33.guardedqueries.testutils.scalacheck.{
  GenFormalInstance,
  GenFormula,
  GenSet
}
import org.scalacheck.Gen
import uk.ac.ox.cs.pdq.fol.Predicate
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.TraverseListGen.traverse
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Atom
import scala.jdk.CollectionConverters._
import com.google.common.collect.ImmutableList
import uk.ac.ox.cs.pdq.fol.Variable
import uk.ac.ox.cs.pdq.fol.TypedConstant

class FilterNestedLoopJoinSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val genSmallPredicateSet = for {
    predicateCount <- Gen.chooseNum(1, 5)
    predicates <- (1 to predicateCount).toList.traverse { index =>
      Gen.chooseNum(1, 4).map { arity => Predicate.create(s"P_${index}", arity) }
    }
  } yield predicates.toSet

  def smallNonExistentialQueryOver(predicateSet: Set[Predicate]): Gen[ConjunctiveQuery] = for {
    querySize <- Gen.chooseNum(1, 5)
    queryAtoms <- Gen.listOfN(
      querySize,
      Gen.oneOf(predicateSet).flatMap { predicate =>
        val genTerm = Gen.oneOf(
          GenFormula.genNumberedVariable(5),
          GenFormula.genConstant(10)
        )
        Gen.listOfN(predicate.getArity(), genTerm).map { terms =>
          Atom.create(predicate, terms*)
        }
      }
    )
    queryVariables = queryAtoms.flatMap(_.getVariables()).toSet
  } yield ConjunctiveQuery.create(queryVariables.toArray, queryAtoms.toArray)

  val genInstanceAndJoinQuery: Gen[(FormalInstance[Constant], ConjunctiveQuery)] = for {
    predicateSet <- genSmallPredicateSet
    constantsToUse = (1 to 10).map(i => TypedConstant.create(s"c_$i"): Constant).toSet
    instance <-
      GenFormalInstance.genFormalInstanceContainingPredicates(predicateSet, constantsToUse)
    query <- smallNonExistentialQueryOver(predicateSet)
  } yield (instance, query)

  "All homomorphisms in FilterNestedLoopJoin.join" should "materialize a subset of the input instance" in {
    forAll(genInstanceAndJoinQuery, minSuccessful(100)) {
      case (instance, query) =>
        val joinAlgorithm = new FilterNestedLoopJoin(c => c)
        joinAlgorithm
          .join(query, instance)
          .allHomomorphisms.asScala
          .foreach { homomorphism =>
            val materializedInstance =
              homomorphism.materializeFunctionFreeAtoms(query.getAtoms().toList.asJava, c => c)
            assert(instance.isSuperInstanceOf(materializedInstance))
          }
    }
  }

  val genQueryAndHomomorphism: Gen[(ConjunctiveQuery, HomomorphicMapping[Constant])] = for {
    predicateSet <- genSmallPredicateSet
    query <- smallNonExistentialQueryOver(predicateSet)
    variablesInQuery = query.getFreeVariables().toSet
    homomorphism <-
      Gen.listOfN(variablesInQuery.size, GenFormula.genConstant(15)).map { constants =>
        new HomomorphicMapping(
          ImmutableList.copyOf(variablesInQuery.toList.asJava),
          ImmutableList.copyOf(constants.asJava)
        )
      }
  } yield (query, homomorphism)

  def equivalentAsHomomorphisms(h1: HomomorphicMapping[Constant],
                                h2: HomomorphicMapping[Constant]
  ): Boolean = {
    def asMap(h: HomomorphicMapping[Constant]): Map[Variable, Constant] =
      h.variableOrdering().asScala.zip(h.orderedMapping().asScala).toMap

    asMap(h1) == asMap(h2)
  }

  "FilterNestedLoopJoin.join" should "find every valid answer" in {
    forAll(genQueryAndHomomorphism, minSuccessful(3000)) {
      case (query, originalHomomorphism) =>
        val materializedInstance = originalHomomorphism
          .materializeFunctionFreeAtoms(query.getAtoms().toList.asJava, c => c)

        assert {
          new FilterNestedLoopJoin(c => c)
            .join(query, materializedInstance)
            .allHomomorphisms
            .asScala
            .exists(equivalentAsHomomorphisms(_, originalHomomorphism))
        }
    }
  }
}
