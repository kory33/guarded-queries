package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance, IncludesFolConstants}
import uk.ac.ox.cs.pdq.fol.{Atom, Constant, Term, Variable}

enum LocalInstanceTerm:
  case LocalName(value: Int)
  case RuleConstant(constant: Constant)

type LocalInstanceTermFact = FormalFact[LocalInstanceTerm]

type LocalInstance = FormalInstance[LocalInstanceTerm]

object LocalInstanceTerm {
  given Extensions: AnyRef with
    import LocalInstanceTerm.*
    extension (t: LocalInstanceTerm)
      def isConstantOrSatisfies(predicate: LocalName => Boolean): Boolean =
        t match
          case name @ LocalName(_)    => predicate(name)
          case RuleConstant(constant) => true

      def mapLocalNamesToTerm(mapper: LocalName => Term): Term =
        t match
          case name @ LocalName(_)    => mapper(name)
          case RuleConstant(constant) => constant

    extension (localInstance: LocalInstance)
      def mapLocalNames(mapper: LocalName => LocalInstanceTerm): LocalInstance =
        localInstance.map {
          case ln: LocalInstanceTerm.LocalName    => mapper(ln)
          case rc: LocalInstanceTerm.RuleConstant => rc
        }

      def activeLocalNames: Set[LocalInstanceTerm.LocalName] =
        localInstance.getActiveTermsIn[LocalInstanceTerm.LocalName]

  def fromTermWithVariableMap(
    term: Term,
    mapper: Variable => LocalInstanceTerm
  ): LocalInstanceTerm =
    term match
      case constant: Constant => RuleConstant(constant)
      case variable: Variable => mapper(variable)
      case _                  => throw new IllegalArgumentException("Unsupported term: " + term)

  given IncludesFolConstants[LocalInstanceTerm] with
    override def includeConstant(constant: Constant): LocalInstanceTerm =
      RuleConstant(constant)
}

object LocalInstanceTermFact {
  def fromAtomWithVariableMap(
    fact: Atom,
    mapper: Variable => LocalInstanceTerm
  ): FormalFact[LocalInstanceTerm] = FormalFact.fromAtom(fact).map(term =>
    LocalInstanceTerm.fromTermWithVariableMap(term, mapper)
  )
}
