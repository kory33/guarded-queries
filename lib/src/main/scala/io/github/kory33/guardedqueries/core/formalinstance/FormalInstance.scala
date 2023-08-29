package io.github.kory33.guardedqueries.core.formalinstance

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Term

import java.util
import java.util.Objects
import scala.reflect.TypeTest
import scala.collection.mutable

case class FormalInstance[TermAlphabet](facts: Set[FormalFact[TermAlphabet]]) {
  lazy val activeTerms: Set[TermAlphabet] = facts.flatMap(_.appliedTerms)

  def getActiveTermsIn[T <: TermAlphabet](using tt: TypeTest[TermAlphabet, T]): Set[T] =
    activeTerms.collect { case tt(subtypeTerm) => subtypeTerm }

  def map[T](mapper: TermAlphabet => T): FormalInstance[T] =
    FormalInstance(facts.map(_.map(mapper)))

  def restrictToAlphabetsWith(predicate: TermAlphabet => Boolean)
    : FormalInstance[TermAlphabet] =
    FormalInstance(facts.filter(_.appliedTerms.forall(predicate)))

  def restrictToSignature(signature: FunctionFreeSignature): FormalInstance[TermAlphabet] =
    FormalInstance(facts.filter(fact => signature.predicates.contains(fact.predicate)))

  def containsFact(fact: FormalFact[TermAlphabet]): Boolean = facts.contains(fact)

  def isSuperInstanceOf(other: FormalInstance[TermAlphabet]): Boolean =
    facts.subsetOf(other.facts)

  def asAtoms(using TermAlphabet =:= Term): Set[Atom] = facts.map(_.asAtom)
}

object FormalInstance {

  def unionAll[TermAlphabet](instances: Iterable[FormalInstance[TermAlphabet]])
    : FormalInstance[TermAlphabet] = {
    var facts = mutable.HashSet.empty[FormalFact[TermAlphabet]]
    for (instance <- instances) { facts ++= instance.facts }
    FormalInstance(facts.toSet)
  }

  def empty[TermAlphabet]: FormalInstance[TermAlphabet] = FormalInstance(Set.empty)

  def of[TermAlphabet](facts: FormalFact[TermAlphabet]*) = FormalInstance(facts.toSet)
}
