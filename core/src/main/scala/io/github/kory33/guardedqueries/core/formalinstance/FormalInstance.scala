package io.github.kory33.guardedqueries.core.formalinstance

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Term

import scala.annotation.targetName
import scala.collection.mutable
import scala.reflect.TypeTest

case class FormalInstance[TermAlphabet](facts: Set[FormalFact[TermAlphabet]]) {
  lazy val activeTerms: Set[TermAlphabet] = facts.flatMap(_.appliedTerms)

  lazy val activePredicates: Set[Predicate] =
    facts.map(_.predicate)

  val allPredicates: Set[Predicate] = facts.map(_.predicate)

  def getActiveTermsIn[T <: TermAlphabet](using tt: TypeTest[TermAlphabet, T]): Set[T] =
    activeTerms.collect { case tt(subtypeTerm) => subtypeTerm }

  @targetName("minus")
  def -(fact: FormalFact[TermAlphabet]): FormalInstance[TermAlphabet] =
    FormalInstance(facts - fact)

  def map[T](mapper: TermAlphabet => T): FormalInstance[T] =
    FormalInstance(facts.map(_.map(mapper)))

  def restrictToAlphabetsWith(predicate: TermAlphabet => Boolean)
    : FormalInstance[TermAlphabet] =
    FormalInstance(facts.filter(_.appliedTerms.forall(predicate)))

  def restrictToSignature(signature: FunctionFreeSignature): FormalInstance[TermAlphabet] =
    FormalInstance(facts.filter(fact => signature.predicates.contains(fact.predicate)))

  def containsFact(fact: FormalFact[TermAlphabet]): Boolean = facts.contains(fact)

  def isSuperInstanceOf(other: FormalInstance[TermAlphabet]): Boolean =
    other.facts.subsetOf(facts)

  @targetName("addFacts")
  def ++(facts: Iterable[FormalFact[TermAlphabet]]): FormalInstance[TermAlphabet] =
    FormalInstance(this.facts ++ facts)

  @targetName("unionInstance")
  def ++(another: FormalInstance[TermAlphabet]): FormalInstance[TermAlphabet] =
    this ++ another.facts

  def asAtoms(using TermAlphabet =:= Term): Set[Atom] = facts.map(_.asAtom)
}

object FormalInstance {

  given Extensions: AnyRef with
    extension [TA](instances: Iterable[FormalInstance[TA]])
      def unionAll: FormalInstance[TA] = {
        val facts = mutable.HashSet.empty[FormalFact[TA]]
        for (instance <- instances) { facts ++= instance.facts }
        FormalInstance(facts.toSet)
      }

  def empty[TermAlphabet]: FormalInstance[TermAlphabet] = FormalInstance(Set.empty)

  def of[TermAlphabet](facts: FormalFact[TermAlphabet]*): FormalInstance[TermAlphabet] =
    FormalInstance(facts.toSet)
}
