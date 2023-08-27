package io.github.kory33.guardedqueries.core.subqueryentailments

import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Variable
import java.util.function.Function

object LocalInstanceTermFact {
  def fromAtomWithVariableMap(fact: Atom,
                              mapper: Function[_ >: Variable, _ <: LocalInstanceTerm]
  ): FormalFact = FormalFact.fromAtom(fact).map(term =>
    LocalInstanceTerm.fromTermWithVariableMap(term, mapper)
  )
}
