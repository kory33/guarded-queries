package io.github.kory33.guardedqueries.core.formalinstance

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Term

import java.util
import java.util.Objects

case class FormalInstance[TermAlphabet](facts: Set[FormalFact[TermAlphabet]]) {
  private lazy val activeTerms: Set[TermAlphabet] =
    Set.copyOf(this.facts.stream.flatMap((fact: FormalFact[TermAlphabet]) =>
      fact.appliedTerms.stream
    ).iterator)

  def getActiveTerms: Set[TermAlphabet] = {
    // TODO remove this getter
    this.activeTerms
  }

  def getActiveTermsInClass[T <: TermAlphabet](clazz: Class[T]): Set[T] =
    Set.copyOf(StreamExtensions.filterSubtype(
      this.getActiveTerms.stream,
      clazz
    ).iterator)

  def map[T](mapper: TermAlphabet => T): FormalInstance[T] =
    FormalInstance.fromIterator(this.facts.stream.map((fact: FormalFact[TermAlphabet]) =>
      fact.map(mapper)
    ).iterator)

  def restrictToAlphabetsWith(predicate: TermAlphabet => Boolean)
    : FormalInstance[TermAlphabet] =
    FormalInstance.fromIterator(this.facts.stream.filter((fact: FormalFact[TermAlphabet]) =>
      fact.appliedTerms.stream.allMatch(predicate(_))
    ).iterator)

  def restrictToSignature(signature: FunctionFreeSignature): FormalInstance[TermAlphabet] =
    FormalInstance.fromIterator(this.facts.stream.filter((fact: FormalFact[TermAlphabet]) =>
      signature.predicates.contains(fact.predicate)
    ).iterator)

  def containsFact(fact: FormalFact[TermAlphabet]): Boolean = this.facts.contains(fact)

  def isSuperInstanceOf(other: FormalInstance[TermAlphabet]): Boolean =
    other.facts.stream.allMatch(this.containsFact)
}

object FormalInstance {
  def apply[TA](fact: util.Iterator[FormalFact[TA]]): FormalInstance[TA] =
    FormalInstance(Set.copyOf(fact))

  def apply[TA](facts: util.Collection[FormalFact[TA]]): FormalInstance[TA] =
    FormalInstance(Set.copyOf(facts))

  def apply[TA](facts: Set[FormalFact[TA]]): FormalInstance[TA] =
    import scala.jdk.CollectionConverters._
    FormalInstance(facts.asJava)

  def asAtoms(instance: FormalInstance[Term]): List[Atom] =
    List.copyOf(instance.facts.stream.map(FormalFact.asAtom).iterator)

  def fromIterator[TermAlphabet](facts: util.Iterator[FormalFact[TermAlphabet]]) =
    new FormalInstance[TermAlphabet](Set.copyOf(facts))

  def unionAll[TermAlphabet](instances: java.lang.Iterable[FormalInstance[TermAlphabet]])
    : FormalInstance[TermAlphabet] = {
    val factSetBuilder = Set.builder[FormalFact[TermAlphabet]]
    instances.forEach((instance: FormalInstance[TermAlphabet]) =>
      factSetBuilder.addAll(instance.facts)
    )
    new FormalInstance[TermAlphabet](factSetBuilder.build)
  }

  def empty[TermAlphabet] = new FormalInstance[TermAlphabet](Set.of)

  def of[TermAlphabet](facts: FormalFact[TermAlphabet]*) =
    new FormalInstance[TermAlphabet](Set.copyOf(facts.toArray))
}
