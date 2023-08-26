package io.github.kory33.guardedqueries.parser

import uk.ac.ox.cs.pdq.fol.Constant
import java.util.function.Function
import java.util.function.Predicate

/**
 * Context for parsing formulae.
 *
 * @param isConstantSymbol
 *   A predicate specifying which symbols are to be regarded as constants. Any symbol yielding
 *   `false` when passed to this function should be regarded as a variable.
 * @param mapConstantSymbolToConstant
 *   A function which maps constant symbols (strings regarded as constants by
 *   [[isConstantSymbol]]) to constants
 */
case class FormulaParsingContext(
  isConstantSymbol: String => Boolean,
  mapConstantSymbolToConstant: String => Constant
)
