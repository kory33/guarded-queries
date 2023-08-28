package io.github.kory33.guardedqueries.core.testcases

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.Variable
import java.util

/**
 * A test case containing GTGD rules and a GTGD-reducible query.
 */
case class GTGDRuleAndGTGDReducibleQuery(guardedRules: util.Collection[_ <: GTGD],
                                         reducibleQuery: GTGDReducibleConjunctiveQuery
) {
  def signatureOfOriginalQuery: FunctionFreeSignature =
    FunctionFreeSignature.encompassingRuleQuery(
      this.guardedRules,
      this.reducibleQuery.originalQuery
    )

  def asGTGDRuleAndConjunctiveQuery: GTGDRuleAndConjunctiveQuery =
    GTGDRuleAndConjunctiveQuery(guardedRules, reducibleQuery.originalQuery)

  def deduplicatedQueryFreeVariables: Set[Variable] =
    asGTGDRuleAndConjunctiveQuery.deduplicatedQueryFreeVariables
}
