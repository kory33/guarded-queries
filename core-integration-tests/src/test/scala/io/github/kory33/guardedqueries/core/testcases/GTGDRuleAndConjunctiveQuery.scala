package io.github.kory33.guardedqueries.core.testcases

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Variable}

/**
 * A test case containing GTGD rules and a conjunctive query.
 */
case class GTGDRuleAndConjunctiveQuery(guardedRules: Set[GTGD], query: ConjunctiveQuery) {
  def signature: FunctionFreeSignature =
    FunctionFreeSignature.encompassingRuleQuery(guardedRules, query)

  def deduplicatedQueryFreeVariables: Set[Variable] =
    query.getFreeVariables.toSet
}
