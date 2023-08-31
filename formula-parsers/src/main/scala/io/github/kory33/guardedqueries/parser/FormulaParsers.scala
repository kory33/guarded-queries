package io.github.kory33.guardedqueries.parser

import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.*

import scala.util.parsing.combinator.*

object FormulaParsers extends RegexParsers {
  // here is the grammar for formulae that we will deal with
  // (we preprocess all input strings and remove all spaces)
  //
  // term ::= <nonempty sequence of characters not including ',' nor ')'>
  // predicate ::= <nonempty sequence of characters not including '(' and not starting with 'EE' or '->'>
  // variable ::= <nonempty sequence of characters not including ',' or '.' which
  //               the parsing context does not judge as constant>
  //
  // terms ::= term | term ',' terms
  // atom ::= predicate '()' | predicate '(' terms ')'
  // atoms ::= atom | atom ',' atoms
  // variables ::= variable | variable ',' variables

  private def term(using context: FormulaParsingContext): Parser[Term] = {
    regex("""[^,)]+""".r).map(termString =>
      if (context.isConstantSymbol(termString)) context.mapConstantSymbolToConstant(termString)
      else Variable.create(termString)
    )
  }

  private val predicate: Parser[String] = {
    for {
      predicateString <- regex("""[^(]+""".r)
      _ <-
        if (predicateString.startsWith("EE") || predicateString.startsWith("->"))
          failure(s"predicate name cannot start with 'EE' or '->', found: $predicateString")
        else
          success(())
    } yield predicateString
  }

  private def atom(using FormulaParsingContext): Parser[Atom] = {
    for {
      predicateName <- predicate
      terms <- literal("(") ~> repsep(term, literal(",")) <~ literal(")")
    } yield {
      val predicate = Predicate.create(predicateName, terms.size)
      Atom.create(predicate, terms.toArray: _*)
    }
  }

  private def existentialVariableBindingsParser(
    using context: FormulaParsingContext
  ): Parser[List[Variable]] = {
    val variableBoundInQuantifier: Parser[Variable] = {
      """[^,.]+""".r.flatMap(variableString =>
        if (context.isConstantSymbol(variableString))
          failure(s"a constant symbol $variableString cannot be bound in quantifier")
        else
          success(Variable.create(variableString))
      )
    }

    for {
      variables <-
        literal("EE") ~> repsep(variableBoundInQuantifier, ",") <~ literal(".")
      _ <-
        if (variables.distinct.size != variables.size)
          failure(s"existential variables must be distinct, found $variables")
        else
          success(())
    } yield variables
  }

  // We use the following grammar for TGDs
  //
  // gtgdHead ::= atoms | 'EE' variables '.' atoms
  // gtgd ::= atoms '->' gtgdHead

  private def tgdParser(using FormulaParsingContext): Parser[TGD] = {
    case class TGDHead(headAtoms: List[Atom], existentialVariables: List[Variable])

    val atoms = repsep(atom, literal(","))

    val existentialHeadParser =
      for {
        existentialVariables <- existentialVariableBindingsParser
        headAtoms <- atoms
      } yield TGDHead(headAtoms, existentialVariables)

    val nonExistentialHeadParser =
      for {
        headAtoms <- atoms
      } yield TGDHead(headAtoms, List.empty)

    val headParser = existentialHeadParser | nonExistentialHeadParser

    for {
      bodyAtoms <- atoms
      _ <- literal("->")
      head <- headParser

      tgd = TGD.create(bodyAtoms.toArray, head.headAtoms.toArray)
      declaredExistentialVariables = head.existentialVariables.toSet
      foundExistentialVariables = tgd.getExistential.toSet
      _ <-
        if (foundExistentialVariables != declaredExistentialVariables)
          failure(
            "existential variables in head do not match those in body: " +
              s"declared: $declaredExistentialVariables, found: $foundExistentialVariables"
          )
        else
          success(())
    } yield tgd
  }

  def gtgdParser(using FormulaParsingContext): Parser[GTGD] = {
    tgdParser.map { tgd =>
      import scala.jdk.CollectionConverters.*

      GTGD(
        tgd.getBodyAtoms.toSet.asJava,
        tgd.getHeadAtoms.toSet.asJava
      )
    }
  }

  def conjunctiveQueryParser(using FormulaParsingContext): Parser[ConjunctiveQuery] = {
    val existentialQueryParser =
      for {
        existentialVariables <- existentialVariableBindingsParser
        queryAtoms <- repsep(atom, literal(","))
      } yield {
        val atomVariables = queryAtoms.flatMap(_.getFreeVariables).toSet
        val freeVariables = atomVariables -- existentialVariables

        ConjunctiveQuery.create(freeVariables.toArray, queryAtoms.toArray)
      }

    val nonExistentialQueryParser =
      for {
        queryAtoms <- repsep(atom, literal(","))
      } yield {
        val variablesInAtoms = queryAtoms.flatMap(_.getFreeVariables).toSet
        ConjunctiveQuery.create(variablesInAtoms.toArray, queryAtoms.toArray)
      }

    existentialQueryParser | nonExistentialQueryParser
  }

  case class IgnoreWhitespaces[FormulaClass](parser: Parser[FormulaClass]) {
    def parse(formulaString: String): FormulaClass = {
      val spaceRemoved = formulaString.replace(" ", "")

      FormulaParsers.parse(parser, spaceRemoved) match {
        case Success(result, _) => result
        case n: NoSuccess =>
          throw new IllegalArgumentException(
            s"invalid formula string: $formulaString, error: ${n.msg}"
          )
      }
    }
  }
}
