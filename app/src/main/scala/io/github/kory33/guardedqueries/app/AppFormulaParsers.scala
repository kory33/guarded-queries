package io.github.kory33.guardedqueries.app

import io.github.kory33.guardedqueries.parser.FormulaParsers
import io.github.kory33.guardedqueries.parser.FormulaParsingContext
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.TypedConstant

object AppFormulaParsers {
  val context = new FormulaParsingContext(
    // we regard all symbols of the form "c_{number}" as constants
    (s: String) => s.startsWith("c_") && s.length > 2 && s.substring(2).chars.allMatch(Character.isDigit),
    TypedConstant.create
  )

  val conjunctiveQuery = new FormulaParsers.WhitespaceIgnoringParser[ConjunctiveQuery](
    FormulaParsers.conjunctiveQueryParser(context)
  )

  val gtgd = new FormulaParsers.WhitespaceIgnoringParser[GTGD](FormulaParsers.gtgdParser(context))
}