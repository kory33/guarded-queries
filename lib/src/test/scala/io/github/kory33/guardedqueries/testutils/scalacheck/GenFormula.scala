package io.github.kory33.guardedqueries.testutils.scalacheck

import io.github.kory33.guardedqueries.testutils.scalacheck.utils.ShrinkList
import org.scalacheck.*
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Conjunction
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.TypedConstant
import uk.ac.ox.cs.pdq.fol.Variable

object GenFormula {
  def genNumberedVariable(indexUpperLimit: Int): Gen[Variable] = for {
    number <- Gen.choose(0, indexUpperLimit)
  } yield Variable.create(s"x_$number")

  def genConstant(indexUpperLimit: Int): Gen[Constant] = for {
    number <- Gen.choose(0, indexUpperLimit)
  } yield TypedConstant.create(s"c_$number")

  def genPredicate(maxArity: Int, indexUpperLimit: Int): Gen[Predicate] = for {
    arity <- Gen.choose(0, maxArity)
    number <- Gen.choose(0, indexUpperLimit)
  } yield Predicate.create(s"P_$number", arity)

  def genPrefixedVariableOrConstant: Gen[Term] =
    Gen.oneOf(genNumberedVariable(30), genConstant(30))

  def genAtom(maxArity: Int, genTerm: Gen[Term]): Gen[Atom] = for {
    predicate <- genPredicate(maxArity, 30)
    terms <- Gen.listOfN(predicate.getArity, genTerm)
  } yield Atom.create(predicate, terms: _*)

  def genConjunctiveQuery(maxAtoms: Int, maxArity: Int): Gen[ConjunctiveQuery] = for {
    numberOfAtoms <- Gen.choose(1, maxAtoms)
    atoms <- Gen.listOfN(numberOfAtoms, genAtom(maxArity, genPrefixedVariableOrConstant))
    variablesInAtoms = atoms.flatMap(_.getVariables()).toSet
    freeVariables <- GenSet.chooseSubset(variablesInAtoms)
  } yield ConjunctiveQuery.create(freeVariables.toArray, atoms.toArray)

  def genExistentialFreeConjunctiveQuery(maxAtoms: Int, maxArity: Int): Gen[ConjunctiveQuery] =
    for {
      numberOfAtoms <- Gen.choose(1, maxAtoms)
      atoms <- Gen.listOfN(numberOfAtoms, genAtom(maxArity, genPrefixedVariableOrConstant))
      variablesInAtoms = atoms.flatMap(_.getVariables()).toSet
    } yield ConjunctiveQuery.create(variablesInAtoms.toArray, atoms.toArray)
}

object ShrinkFormula {
  def shrinkNumberInString(prefix: String, string: String): LazyList[String] =
    try {
      // shrink the number if the input string is of the form "$prefix$number"
      val number = string.drop(prefix.length).toInt
      LazyList.from(Shrink.shrink(number)).map(number => s"$prefix$number")
    } catch {
      // otherwise shrink string
      case _: NumberFormatException => LazyList.from(Shrink.shrink(string))
    }

  given Shrink[Variable] = Shrink.withLazyList { variable =>
    shrinkNumberInString("x_", variable.getSymbol)
      .map(shrunkSymbol => Variable.create(shrunkSymbol))
  }

  given Shrink[Constant] = Shrink.withLazyList {
    case constant: TypedConstant if constant.getType == classOf[String] =>
      shrinkNumberInString("c_", constant.value.asInstanceOf[String])
        .map(shrunkSymbol => TypedConstant.create(shrunkSymbol))
    case _ => LazyList.empty
  }

  given Shrink[Term] = Shrink.withLazyList {
    case variable: Variable => LazyList.from(Shrink.shrink(variable))
    case constant: Constant => LazyList.from(Shrink.shrink(constant))
    case _                  => LazyList.empty
  }

  given Shrink[Predicate] = Shrink.withLazyList { predicate =>
    for {
      shrunkArity <- LazyList.from(Shrink.shrink(predicate.getArity)).filter(_ >= 0)
    } yield Predicate.create(predicate.getName, shrunkArity)
  }

  given Shrink[Atom] = Shrink.withLazyList { atom =>
    for {
      shrunkPredicate <- LazyList.from(Shrink.shrink(atom.getPredicate))
      prefixTerms = atom.getTerms().take(shrunkPredicate.getArity)
      shrunkTerms <- ShrinkList.shrinkEachIn(prefixTerms.toList)
    } yield Atom.create(shrunkPredicate, shrunkTerms: _*)
  }

  given Shrink[ConjunctiveQuery] = Shrink.withLazyList { cq =>
    for {
      shrunkConjunctionSize <- LazyList.from(Shrink.shrink(cq.getAtoms.length)).filter(_ > 0)
      shrunkConjunction = cq.getAtoms.take(shrunkConjunctionSize)
      shrunkAtoms <- ShrinkList.shrinkEachIn(shrunkConjunction.toList)
      freeVariables = shrunkAtoms.flatMap(_.getVariables()).toSet
      shrunkFreeVariables <- Shrink.shrinkContainer[Set, Variable].shrink(freeVariables)
    } yield ConjunctiveQuery.create(cq.getFreeVariables, shrunkAtoms.toArray)
  }
}
