package io.github.kory33.guardedqueries.core.testcases

import io.github.kory33.guardedqueries.parser.{FormulaParsers, FormulaParsingContext}
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, TypedConstant}

object TestCaseParsers {
  val commonParsingContext: FormulaParsingContext =
    FormulaParsingContext(
      // we regard all symbols of the form "c_{number}" as constants
      (s: String) =>
        s.startsWith("c_") && s.length > 2 && s.substring(2).chars.allMatch(Character.isDigit),
      TypedConstant.create
    )

  val conjunctiveQuery: FormulaParsers.IgnoreWhitespaces[ConjunctiveQuery] =
    FormulaParsers.IgnoreWhitespaces(
      FormulaParsers.conjunctiveQueryParser(using commonParsingContext)
    )

  val gtgd: FormulaParsers.IgnoreWhitespaces[GTGD] =
    FormulaParsers.IgnoreWhitespaces(FormulaParsers.gtgdParser(using commonParsingContext))
}
