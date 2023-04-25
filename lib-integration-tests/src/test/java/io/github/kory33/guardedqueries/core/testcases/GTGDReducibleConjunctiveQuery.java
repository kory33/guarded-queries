package io.github.kory33.guardedqueries.core.testcases;

import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

import java.util.Collection;

/**
 * A conjunctive query that can be "GYO-reduced" to an atomic query with help
 * of additional GTGD rules.
 * <p>
 * An object of this class has the reduced query (and additional rules)
 * manually specified by the user. <strong>It is upto the user's responsibility to
 * ensure that the pair of {@code reductionRules} and {@code reducedAtomicQuery}
 * is indeed equivalent to {@code originalQuery}.</strong>
 * <p>
 * The reduction is performed by eliminating existential variables in the original query
 * while adding new guarded rules to compensate for the elimination. For example, consider the
 * following query as the original query:
 * <pre>
 *     Q_0 = ∃x,y,z. T(w,x,y) ∧ S(x,y) ∧ T(y,z,w)
 * </pre>
 * We can add a guarded rule {@code T(y,z,w) → I_1(y,w)} to make the query
 * <pre>
 *     Q_1 = ∃x,y. T(w,x,y) ∧ S(x,y) ∧ I_1(y,w)
 * </pre>
 * equivalent to the original query {@code Q_0}. Note that we successfully eliminated {@code z}
 * from the query. We then add a rule {@code T(w,x,y) ∧ S(x,y) ∧ I_1(y,w) → I_2(w)} to make the
 * query
 * <pre>
 *     Q_2 = I_2(w)
 * </pre>
 * equivalent to {@code Q_1} and hence {@code Q_0}.
 * <p>
 * Now that all existential variables
 * are eliminated from the query, GSat is able to Datalog-saturate the
 * rule (plus the additional two rules deducing {@code I_1} and {@code I_2}),
 * allowing us to extract all answers to {@code w} in {@code Q_0}.
 */
public record GTGDReducibleConjunctiveQuery(
        ConjunctiveQuery originalQuery,
        Collection<? extends GTGD> reductionRules,
        ConjunctiveQuery existentialFreeQuery
) {
    public GTGDReducibleConjunctiveQuery {
        if (existentialFreeQuery.getBoundVariables().length != 0) {
            throw new IllegalArgumentException("existentialFreeQuery must be existential-free");
        }
    }
}
