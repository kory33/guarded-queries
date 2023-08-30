package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Variable

object LocalInstanceTermFact {
  def fromAtomWithVariableMap(
    fact: Atom,
    mapper: Variable => LocalInstanceTerm
  ): FormalFact[LocalInstanceTerm] = FormalFact.fromAtom(fact).map(term =>
    LocalInstanceTerm.fromTermWithVariableMap(term, mapper)
  )
}
