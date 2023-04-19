package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormula
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import uk.ac.ox.cs.pdq.fol.Atom

class ConjunctiveQueryExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import io.github.kory33.guardedqueries.testutils.scalacheck.FormulaShrink.given

  val largeCQ: Gen[ConjunctiveQuery] = GenFormula.genConjunctiveQuery(50, 10)

  // apply a given Int => Boolean function on the predicate number
  // on generated atoms
  def applyToPredicateNumber(f: Int => Boolean)(atom: Atom): Boolean = {
    val name = atom.getPredicate().getName()
    name.startsWith("P_") && f(name.drop(2).toInt)
  }

  ".filterAtom" should "only contain atoms in the input query" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))

      whenever(result.isPresent) {
        val filteredAtoms = result.get().getAtoms()
        assert(filteredAtoms.forall(atom => cq.getAtoms().contains(atom)))
      }
    }
  }

  ".filterAtom" should "contain all and only atoms that satisfy the predicate" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))
      val atomsInResult = result.map(_.getAtoms()).orElse(Array.empty).toList
      val atomsInInput = cq.getAtoms().toList

      assert {
        atomsInResult.forall { atom =>
          applyToPredicateNumber(f)(atom) == atomsInResult.contains(atom)
        }
      }
    }
  }

  ".filterAtom" should "not change variable boundedness" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))
      val atomsInResult = result.map(_.getAtoms()).orElse(Array.empty).toList
      val variablesInResult = atomsInResult.flatMap(_.getVariables().toList)

      assert {
        variablesInResult.forall(v =>
          cq.getBoundVariables().contains(v) == result.get().getBoundVariables().contains(v)
        )
      }
    }
  }
}
