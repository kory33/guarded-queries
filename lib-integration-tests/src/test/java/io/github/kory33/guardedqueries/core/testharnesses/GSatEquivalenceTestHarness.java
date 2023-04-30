package io.github.kory33.guardedqueries.core.testharnesses;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.time.Instant;
import java.util.Date;

import static io.github.kory33.guardedqueries.core.testharnesses.InstanceGeneration.randomInstanceOver;

/**
 * A test harness to compare the rewriting outputs of GSat and of our implementation
 * on {@link GTGDRuleAndGTGDReducibleQuery} instances.
 */
public record GSatEquivalenceTestHarness(AbstractSaturation<? extends GTGD> gsatImplementation,
                                         GuardedRuleAndQueryRewriter rewriterToBeTested) {

    private static void logWithTime(String message) {
        System.out.println("[" + Date.from(Instant.now()) + "] " + message);
    }

    private record RewriteResultsToBeCompared(
            DatalogProgram gsatRewriting,
            ConjunctiveQuery gsatQuery,
            DatalogRewriteResult ourRewriting,

            // the atom to which we can materialize computed homomorphisms
            Atom answerAtom
    ) {
        public FormalInstance<Constant> answersWithGsatRewriting(
                final FormalInstance<Constant> testInstance
        ) {
            final var gsatSaturatedInstance = new NaiveSaturationEngine()
                    .saturateInstance(gsatRewriting, testInstance, c -> c);

            return new FormalInstance<>(
                    new FilterNestedLoopJoin<>(c -> c)
                            .join(gsatQuery, gsatSaturatedInstance)
                            .materializeFunctionFreeAtom(answerAtom, c -> c)
            );
        }

        public FormalInstance<Constant> answersWithOurRewriting(
                final FormalInstance<Constant> testInstance
        ) {
            return RunOutputDatalogProgram.answersOn(testInstance, ourRewriting, answerAtom, c -> c);
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

    private RewriteResultsToBeCompared rewriteInTwoMethods(
            final GTGDRuleAndGTGDReducibleQuery ruleQuery
    ) {
        logWithTime("Rewriting " + ruleQuery.reducibleQuery().originalQuery());

        final var gsatRewritingStart = System.nanoTime();
        final var gsatRewriting = DatalogProgram.tryFromDependencies(
                gsatImplementation.run(
                        ImmutableList.<Dependency>builder()
                                .addAll(ruleQuery.guardedRules())
                                .addAll(ruleQuery.reducibleQuery().reductionRules())
                                .build()
                )
        );
        logWithTime("Done Gsat rewriting in " + (System.nanoTime() - gsatRewritingStart) + " ns");

        final var ourRewritingStart = System.nanoTime();
        final var ourRewriting = rewriterToBeTested
                .rewrite(ruleQuery.guardedRules(), ruleQuery.reducibleQuery().originalQuery());
        logWithTime("Done guarded-query rewriting in " + (System.nanoTime() - ourRewritingStart) + " ns");

        final var gsatQuery = ruleQuery.reducibleQuery().existentialFreeQuery();
        final var minimizedRewriting = minimizeRewriteResultAndLogIntermediateCounts(ourRewriting);
        final var answerAtom = Atom.create(
                Predicate.create("Answer", ruleQuery.deduplicatedQueryFreeVariables().size()),
                ruleQuery.deduplicatedQueryFreeVariables().toArray(Variable[]::new)
        );

        return new RewriteResultsToBeCompared(gsatRewriting, gsatQuery, minimizedRewriting, answerAtom);
    }

    /**
     * Test that {@link #gsatImplementation} and {@link #rewriterToBeTested} produce
     * the equivalent Datalog rewritings on the given {@link GTGDRuleAndGTGDReducibleQuery}.
     * <p>
     * The test is repeatedly performed on randomly generated database instances
     * (with the number of test rounds being specified by {@code instanceGenerationRoundCount}).
     */
    public void checkThatGSatAndTheRewriterAgreeOn(
            final GTGDRuleAndGTGDReducibleQuery ruleQuery,
            final int instanceGenerationRoundCount
    ) {
        final var rewritings = rewriteInTwoMethods(ruleQuery);

        for (int i = 0; i < instanceGenerationRoundCount; i++) {
            final var testInstance = randomInstanceOver(ruleQuery.signatureOfOriginalQuery());

            final var gsatAnswer = rewritings.answersWithGsatRewriting(testInstance);
            final var ourAnswer = rewritings.answersWithOurRewriting(testInstance);

            if (!gsatAnswer.equals(ourAnswer)) {
                throw new AssertionError("GSat and our answer differ! " +
                        "input = " + testInstance + ", " +
                        "gsatAnswer = " + gsatAnswer + ", " +
                        "ourAnswer = " + ourAnswer
                );
            } else if (i % 20 == 0) {
                logWithTime("Test " + i + " passed, " +
                        "input size = " + testInstance.facts.size() + ", " +
                        "answer size = " + gsatAnswer.facts.size()
                );
            }
        }
    }
}
