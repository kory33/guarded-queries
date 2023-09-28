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
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.TypedConstant
import uk.ac.ox.cs.pdq.fol.Variable

import scala.jdk.CollectionConverters.*

class SingleAtomMatchingSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import io.github.kory33.guardedqueries.core.formalinstance.CQAsFormalInstance.given

  val genSmallAtom: Gen[Atom] = GenFormula.genAtom(
    4,
    Gen.oneOf(GenFormula.genNumberedVariable(30), GenFormula.genConstant(4))
  )

  val genSignatureAndFormalInstance: Gen[(Set[Predicate], FormalInstance[Constant])] = for {
    predicateCount <- Gen.chooseNum(1, 10)
    predicates <- (1 to predicateCount).toList.traverse { index =>
      Gen.chooseNum(1, 4).map { arity => Predicate.create(s"P_$index", arity) }
    }
    predicateSet = predicates.toSet

    constantsCount = predicates.map(_.getArity()).maxOption.getOrElse(2) * 3
    constantsToUse =
      (1 to constantsCount).map(i => TypedConstant.create(s"c_$i"): Constant).toSet

    instance <-
      GenFormalInstance.genFormalInstanceContainingPredicates(predicateSet, constantsToUse)
  } yield (predicateSet, instance)

  val genInstanceAndQuery: Gen[(FormalInstance[Constant], Atom)] =
    genSignatureAndFormalInstance.flatMap {
      case (predicateSet, instance) =>
        for {
          predicate <- Gen.oneOf(predicateSet)
          terms <- Gen.listOfN(
            predicate.getArity,
            Gen.oneOf(GenFormula.genNumberedVariable(4), GenFormula.genConstant(4))
          )
        } yield (instance, Atom.create(predicate, terms*))
    }

  "SingleAtomMatching.allMatches.materializeFunctionFreeAtom" should "only include formal facts having the same atom as query atom" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) {
      case (instance, query) =>
        SingleAtomMatching
          .allMatches(query.asQueryLikeAtom, instance)
          .materializeFunctionFreeAtom(query)
          .foreach { fact => assert(fact.predicate.equals(query.getPredicate)) }
    }
  }

  it should "be a subset of input instance" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) {
      case (instance, query) =>
        SingleAtomMatching
          .allMatches(query.asQueryLikeAtom, instance)
          .materializeFunctionFreeAtom(query)
          .foreach { fact => assert(instance.facts.contains(fact)) }
    }
  }

  it should "be idempotent" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) {
      case (instance, query) =>
        val firstMatch = SingleAtomMatching
          .allMatches(query.asQueryLikeAtom, instance)
          .materializeFunctionFreeAtom(query)
          .toSet

        val secondMatch = SingleAtomMatching
          .allMatches(query.asQueryLikeAtom, FormalInstance(firstMatch))
          .materializeFunctionFreeAtom(query)

        assert(firstMatch == secondMatch.toSet)
    }
  }

  def genAtomAndHomomorphism: Gen[(Atom, HomomorphicMapping[Variable, Constant])] = for {
    atom <- genSmallAtom
    variablesInAtom = atom.getVariables.toSet.toList
    homomorphism <- Gen.listOfN(variablesInAtom.size, GenFormula.genConstant(10))
  } yield (atom, HomomorphicMapping(variablesInAtom, homomorphism))

  it should "find every valid answer" in {
    // We test that for every pair of query and a homomorphism,
    // the algorithm successfully finds the mapped tuple as the answer.
    //
    // For instance, for an atomic query A(x,c) we might prepare a homomorphism {x -> d}.
    // When we run the matching algorithm on the instance {A(d,c)}, which is the result of
    // materializing the atom according to the homomorphism, we expect the algorithm to
    // return {x -> d} as the only answer, and that the instance materialized from the answer
    // is equal to {A(d,c)}.
    //
    // Assuming that the algorithm is monotonic and affine in the input instance (i.e. the
    // presence of a homomorphism in the answer only depends on the existence of the corresponding
    // tuple in the input instance), this test should be sufficient to show the correctness of
    // the algorithm.
    forAll(genAtomAndHomomorphism, minSuccessful(1000)) {
      case (atom, homomorphism) =>
        val instanceContainingJustTheMaterializedAtom =
          FormalInstance.of(homomorphism.materializeFunctionFreeAtom(atom))

        val matches = SingleAtomMatching
          .allMatches(atom.asQueryLikeAtom, instanceContainingJustTheMaterializedAtom)
          .materializeFunctionFreeAtom(atom)
          .toSet

        val matchInstance = FormalInstance(matches)

        assert(matchInstance == instanceContainingJustTheMaterializedAtom)
    }
  }
}
