package io.github.kory33.guardedqueries.core.testharnesses;

import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndConjunctiveQuery;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.time.Instant;
import java.util.Date;

import static io.github.kory33.guardedqueries.core.testharnesses.InstanceGeneration.randomInstanceOver;

public record RewriterEquivalenceTestHarness(
        GuardedRuleAndQueryRewriter firstRewriter,
        GuardedRuleAndQueryRewriter secondRewriter
) {
    private static void logWithTime(String message) {
        System.out.println("[" + Date.from(Instant.now()) + "] " + message);
    }

    private record RewriteResultsToBeCompared(
            DatalogRewriteResult resultFromFirstRewriter,
            DatalogRewriteResult resultFromSecondRewriter,
            // the atom to which we can materialize computed homomorphisms
            Atom answerAtom
    ) {
        public FormalInstance<Constant> answerWithFirstRewriter(
                final FormalInstance<Constant> testInstance
        ) {
            return RunOutputDatalogProgram.answersOn(testInstance, resultFromFirstRewriter, answerAtom, c -> c);
        }

        public FormalInstance<Constant> answerWithSecondRewriter(
                final FormalInstance<Constant> testInstance
        ) {
            return RunOutputDatalogProgram.answersOn(testInstance, resultFromSecondRewriter, answerAtom, c -> c);
        }
    }

    private DatalogRewriteResult minimizeRewriteResultAndLogIntermediateCounts(
            DatalogRewriteResult originalRewriteResult
    ) {
        logWithTime("# of subgoal derivation rules in original output: " +
                originalRewriteResult.subgoalAndGoalDerivationRules().rules().size());

        final var minimalExactBodyMinimizedRewriting = originalRewriteResult
                .minimizeSubgoalDerivationRulesUsing(MinimalExactBodyDatalogRuleSet::new);

        logWithTime("# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " +
                minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        final var minimizedRewriting = minimalExactBodyMinimizedRewriting
                .minimizeSubgoalDerivationRulesUsing(MinimallyUnifiedDatalogRuleSet::new);

        logWithTime("# of subgoal derivation rules in minimizedRewriting: " +
                minimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        return minimizedRewriting;
    }

    private RewriteResultsToBeCompared rewriteUsingTwoRewriters(
            final GTGDRuleAndConjunctiveQuery ruleQuery
    ) {
        logWithTime("Rewriting " + ruleQuery.query());

        logWithTime("Rewriting with " + firstRewriter + "...");
        final var firstRewritingStart = System.nanoTime();
        final var firstRewriting = firstRewriter.rewrite(ruleQuery.guardedRules(), ruleQuery.query());
        logWithTime("Done rewriting with " + firstRewriter + " in " + (System.nanoTime() - firstRewritingStart) + "ns");
        final var minimizedFirstRewriting = minimizeRewriteResultAndLogIntermediateCounts(firstRewriting);

        logWithTime("Rewriting with " + secondRewriter + "...");
        final var secondRewritingStart = System.nanoTime();
        final var secondRewriting = secondRewriter.rewrite(ruleQuery.guardedRules(), ruleQuery.query());
        logWithTime("Done rewriting with " + secondRewriter + " in " + (System.nanoTime() - secondRewritingStart) + "ns");
        final var minimizedSecondRewriting = minimizeRewriteResultAndLogIntermediateCounts(secondRewriting);

        final var answerAtom = Atom.create(
                Predicate.create("Answer", ruleQuery.deduplicatedQueryFreeVariables().size()),
                ruleQuery.deduplicatedQueryFreeVariables().toArray(Variable[]::new)
        );

        return new RewriteResultsToBeCompared(minimizedFirstRewriting, minimizedSecondRewriting, answerAtom);
    }

    /**
     * Test that the two {@link GuardedRuleAndQueryRewriter}s produce
     * equivalent Datalog rewritings on the given {@link GTGDRuleAndGTGDReducibleQuery}.
     * <p>
     * The test is repeatedly performed on randomly generated database instances
     * (with the number of test rounds being specified by {@code instanceGenerationRoundCount}).
     */
    public void checkThatTwoRewritersAgreeOn(
            final GTGDRuleAndConjunctiveQuery ruleQuery,
            final int instanceGenerationRoundCount
    ) {
        final var rewritings = rewriteUsingTwoRewriters(ruleQuery);

        for (int i = 0; i < instanceGenerationRoundCount; i++) {
            final var testInstance = randomInstanceOver(ruleQuery.signature());

            final var firstAnswer = rewritings.answerWithFirstRewriter(testInstance);
            final var secondAnswer = rewritings.answerWithSecondRewriter(testInstance);

            if (!firstAnswer.equals(secondAnswer)) {
                throw new AssertionError("Two rewriters gave different answers! " +
                        "input = " + testInstance + ", " +
                        "first rewriter answer = " + firstAnswer + ", " +
                        "second rewriter answer = " + secondAnswer + ", " +
                        "first rewriter = " + firstRewriter + ", " +
                        "second rewriter = " + secondRewriter
                );
            } else {
                logWithTime("Test " + i + " passed, " +
                        "input size = " + testInstance.facts.size() + ", " +
                        "answer size = " + firstAnswer.facts.size()
                );
            }
        }
    }

    /**
     * Test that the two {@link GuardedRuleAndQueryRewriter}s produce
     * equivalent Datalog rewritings on the given {@link GTGDRuleAndGTGDReducibleQuery}.
     * <p>
     * The test is repeatedly performed on randomly generated database instances
     * (with the number of test rounds being specified by {@code instanceGenerationRoundCount}).
     */
    public void checkThatTwoRewritersAgreeOn(
            final GTGDRuleAndGTGDReducibleQuery ruleQuery,
            final int instanceGenerationRoundCount
    ) {
        checkThatTwoRewritersAgreeOn(ruleQuery.asGTGDRuleAndConjunctiveQuery(), instanceGenerationRoundCount);
    }
}
