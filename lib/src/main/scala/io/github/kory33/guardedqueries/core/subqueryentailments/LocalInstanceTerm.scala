package io.github.kory33.guardedqueries.core.subqueryentailments

import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.Variable
import java.util.function.Function
import java.util.function.Predicate

sealed trait LocalInstanceTerm {
  def isConstantOrSatisfies(predicate: Predicate[_ >: LocalInstanceTerm.LocalName]): Boolean =
    this match
      case _: LocalInstanceTerm.RuleConstant => true
      case _: LocalInstanceTerm.LocalName    => predicate.test(localName)
      case _ => throw new IllegalStateException("Unreachable: " + this)

  def mapLocalNamesToTerm(mapper: Function[_ >: LocalInstanceTerm.LocalName, _ <: Term]): Term
}

object LocalInstanceTerm {
  case class LocalName(value: Int) extends LocalInstanceTerm {
    override def mapLocalNamesToTerm(mapper: Function[
      _ >: LocalInstanceTerm.LocalName,
      _ <: Term
    ]): Term = mapper.apply(this)

    override def toString: String = "LocalName[" + value + "]"
  }

  case class RuleConstant(constant: Constant) extends LocalInstanceTerm {
    override def mapLocalNamesToTerm(mapper: Function[
      _ >: LocalInstanceTerm.LocalName,
      _ <: Term
    ]): Term = this.constant
    override def toString: String = "RuleConstant[" + constant + "]"
  }

  def fromTermWithVariableMap(term: Term,
                              mapper: Function[_ >: Variable, _ <: LocalInstanceTerm]
  ): LocalInstanceTerm =
    term match
      case _: Constant => RuleConstant(constant)
      case _: Variable => mapper.apply(variable)
      case _           => throw new IllegalArgumentException("Unsupported term: " + term)
}
