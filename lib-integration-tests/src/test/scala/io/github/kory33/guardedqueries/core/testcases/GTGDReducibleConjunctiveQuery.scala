package io.github.kory33.guardedqueries.core.testcases

import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import java.util

/**
 * A conjunctive query that can be "GTGD-reduced" to an atomic query with help of additional
 * GTGD rules.
 *
 * An object of this class has the reduced query (and additional rules) manually specified by
 * the user. <strong>It is upto the user's responsibility to ensure that the pair of
 * `reductionRules` and `reducedAtomicQuery` is indeed equivalent to `originalQuery`.</strong>
 *
 * The reduction is performed by eliminating existential variables in the original query while
 * adding new guarded rules to compensate for the elimination. For example, consider the
 * following query as the original query:
 *
 * <pre> Q_0 = ∃x,y,z. T(w,x,y) ∧ S(x,y) ∧ T(y,z,w) </pre>
 *
 * We can add a guarded rule `T(y,z,w) → I_1(y,w)` to make the query
 *
 * <pre> Q_1 = ∃x,y. T(w,x,y) ∧ S(x,y) ∧ I_1(y,w) </pre>
 *
 * equivalent to the original query `Q_0`. Note that we successfully eliminated `z` from the
 * query. We then add a rule `T(w,x,y) ∧ S(x,y) ∧ I_1(y,w) → I_2(w)` to make the query
 *
 * <pre> Q_2 = I_2(w) </pre>
 *
 * equivalent to `Q_1` and hence to `Q_0`.
 *
 * Now that all existential variables are eliminated from the query, GSat is able to
 * Datalog-saturate the rule (plus the additional two rules deducing `I_1` and `I_2`), allowing
 * us to extract all answers to `w` in `Q_0`.
 */
case class GTGDReducibleConjunctiveQuery(
  originalQuery: ConjunctiveQuery,
  reductionRules: util.Collection[_ <: GTGD],
  existentialFreeQuery: ConjunctiveQuery
) {
  if (existentialFreeQuery.getBoundVariables.length != 0)
    throw new IllegalArgumentException("existentialFreeQuery must be existential-free")
}
