package io.github.kory33.guardedqueries.core.formalinstance

import com.google.common.collect.ImmutableList
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Term

case class FormalFact[TermAlphabet](predicate: Predicate,
                                    appliedTerms: ImmutableList[TermAlphabet]
) {
  def map[T](mapper: TermAlphabet => T) = new FormalFact[T](
    this.predicate,
    ImmutableList.copyOf(this.appliedTerms.stream.map(mapper(_)).iterator)
  )

  override def toString: String =
    s"${predicate.toString}(${String.join(", ", appliedTerms.stream.map(_.toString).toList)})"
}

object FormalFact {
  def asAtom(fact: FormalFact[Term]): Atom =
    import scala.jdk.CollectionConverters._
    Atom.create(fact.predicate, fact.appliedTerms.asScala.toArray: _*)

  def fromAtom(atom: Atom) =
    new FormalFact[Term](atom.getPredicate, ImmutableList.copyOf(atom.getTerms))
}
