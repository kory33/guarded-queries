package io.github.kory33.guardedqueries.core.datalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import uk.ac.ox.cs.pdq.fol.Atom;

import java.util.Arrays;
import java.util.HashSet;

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
     * Optimize the set of subgoal derivation rules
     * by removing rules that are subsumed by other rules.
     */
    public DatalogRewriteResult minimizeSubgoalDerivationRulesBySimpleSubsumption() {
        /*
         * A collection of subgoal derivation rules that do not subsume each other (by a simple subsumption).
         */
        class MaximalSubgoalDerivationRuleSet {
            private final HashSet<DatalogRule> rulesKnownToBeMaximalSoFar = new HashSet<>();

            /**
             * See if we can conclude that the first rule subsumes the second rule.
             * <p>
             * Similar to ExactAtomSubsumer, we conclude that a rule A to be subsumed by another rule B if
             *  1. the head of A and of B are the same
             *     (since we are only interested in subsumption of subgoal derivation rules, exact check is enough)
             *  2. the body of A is a superset of the body of B
             */
            private static boolean firstRuleSubsumesSecond(DatalogRule first, DatalogRule second) {
                if (!Arrays.equals(first.getHeadAtoms(), second.getHeadAtoms())) {
                    return false;
                }

                // we check that the body of the first rule is a subset of the body of the second rule
                subsetCheck:
                for (final var firstBodyAtom : first.getBodyAtoms()) {
                    for (final var secondBodyAtom : second.getBodyAtoms()) {
                        if (firstBodyAtom.equals(secondBodyAtom)) {
                            continue subsetCheck;
                        }
                    }
                    return false;
                }
                return true;
            }

            private ImmutableList<DatalogRule> rulesSubsumedBy(DatalogRule rule) {
                return ImmutableList.copyOf(
                        rulesKnownToBeMaximalSoFar.stream()
                                .filter(otherRule -> firstRuleSubsumesSecond(otherRule, rule))
                                .iterator()
                );
            }

            private boolean isSubsumedByExistingRule(DatalogRule rule) {
                return rulesKnownToBeMaximalSoFar.stream()
                        .anyMatch(existingRule -> firstRuleSubsumesSecond(existingRule, rule));
            }

            public void addRule(DatalogRule rule) {
                if (!isSubsumedByExistingRule(rule)) {
                    rulesSubsumedBy(rule).forEach(rulesKnownToBeMaximalSoFar::remove);
                    rulesKnownToBeMaximalSoFar.add(rule);
                }
            }

            public ImmutableSet<DatalogRule> getRules() {
                return ImmutableSet.copyOf(rulesKnownToBeMaximalSoFar);
            }
        }

        final var maximalRuleSet = new MaximalSubgoalDerivationRuleSet();
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
