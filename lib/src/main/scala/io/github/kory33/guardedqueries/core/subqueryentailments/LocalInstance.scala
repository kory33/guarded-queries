package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import uk.ac.ox.cs.pdq.fol.{Atom, Constant, Term, Variable}

sealed trait LocalInstanceTerm {
  def isConstantOrSatisfies(predicate: LocalInstanceTerm.LocalName => Boolean): Boolean =
    this match
      case _: LocalInstanceTerm.RuleConstant      => true
      case localName: LocalInstanceTerm.LocalName => predicate(localName)

  def mapLocalNamesToTerm(mapper: LocalInstanceTerm.LocalName => Term): Term
}

object LocalInstanceTerm {
  case class LocalName(value: Int) extends LocalInstanceTerm {
    override def mapLocalNamesToTerm(mapper: LocalInstanceTerm.LocalName => Term): Term =
      mapper.apply(this)
  }

  case class RuleConstant(constant: Constant) extends LocalInstanceTerm {
    override def mapLocalNamesToTerm(mapper: LocalInstanceTerm.LocalName => Term): Term =
      this.constant
  }

  def fromTermWithVariableMap(
    term: Term,
    mapper: Variable => LocalInstanceTerm
  ): LocalInstanceTerm =
    term match
      case constant: Constant => RuleConstant(constant)
      case variable: Variable => mapper(variable)
      case _                  => throw new IllegalArgumentException("Unsupported term: " + term)
}

object LocalInstanceTermFact {
  def fromAtomWithVariableMap(
    fact: Atom,
    mapper: Variable => LocalInstanceTerm
  ): FormalFact[LocalInstanceTerm] = FormalFact.fromAtom(fact).map(term =>
    LocalInstanceTerm.fromTermWithVariableMap(term, mapper)
  )
}

type LocalInstance = FormalInstance[LocalInstanceTerm]
