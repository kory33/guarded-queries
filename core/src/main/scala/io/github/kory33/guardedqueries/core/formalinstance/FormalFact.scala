package io.github.kory33.guardedqueries.core.formalinstance

import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Term

case class FormalFact[TermAlphabet](predicate: Predicate, appliedTerms: List[TermAlphabet]) {
  def map[T](mapper: TermAlphabet => T) = new FormalFact[T](predicate, appliedTerms.map(mapper))

  override def toString: String =
    s"${predicate.toString}(${appliedTerms.map(_.toString).mkString(", ")})"

  def asAtom(using ev: TermAlphabet =:= Term): Atom =
    Atom.create(predicate, ev.substituteCo(appliedTerms): _*)
}

object FormalFact {
  def fromAtom(atom: Atom) =
    new FormalFact[Term](atom.getPredicate, atom.getTerms.toList)
}
