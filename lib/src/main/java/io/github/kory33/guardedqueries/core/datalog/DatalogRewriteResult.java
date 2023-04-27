package io.github.kory33.guardedqueries.core.datalog;

import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.subsumption.datalog.MaximalDatalogRuleSet;
import uk.ac.ox.cs.pdq.fol.Atom;

/**
 * A datalog program obtained as a result of rewriting a rule set plus a conjunctive query.
 * <p>
 * The program is decomposed into two parts: the input-rule saturation rules and the subgoal and goal derivation rules.
 * The input-rule saturation rules is only used to Datalog-saturate the input database with input rules,
 * while the subgoal and goal derivation rules is used to derive the subgoal atoms and the goal atom
 * and nothing other than that.
 * <p>
 * The goal atom should be the goal predicate applied to free variables in the query (without repetition).
 * For instance, if the input conjunctive query was {@code ∃x. R(x, y) ∧ R(y, z) ∧ S(x, z)}, then the goal atom
 * should be either {@code G(x, z)} or {@code G(z, x)}, where {@code G} is the goal predicate introduced in the
 * rewritten program.
 */
public record DatalogRewriteResult(
        DatalogProgram inputRuleSaturationRules,
        DatalogProgram subgoalAndGoalDerivationRules,
        Atom goal
) {
    /**
     * Optimize the set of subgoal derivation rules by removing rules that are subsumed by other rules.
     */
    public <S extends MaximalDatalogRuleSet> DatalogRewriteResult minimizeSubgoalDerivationRulesUsing(
            final MaximalDatalogRuleSet.Factory<S> maximalDatalogRuleSetFactory
    ) {
        final var maximalRuleSet = maximalDatalogRuleSetFactory.emptyRuleSet();
        for (DatalogRule rule : this.subgoalAndGoalDerivationRules.rules()) {
            maximalRuleSet.addRule(rule);
        }
        return new DatalogRewriteResult(
                this.inputRuleSaturationRules,
                new DatalogProgram(maximalRuleSet.getRules()),
                this.goal
        );
    }
}
