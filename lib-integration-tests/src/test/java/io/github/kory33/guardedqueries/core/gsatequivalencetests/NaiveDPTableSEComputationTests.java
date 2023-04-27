package io.github.kory33.guardedqueries.core.gsatequivalencetests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.NaiveDPTableSEComputation;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases;
import io.github.kory33.guardedqueries.core.utils.MappingStreams;
import org.junit.jupiter.api.Test;
import uk.ac.ox.cs.gsat.GSat;
import uk.ac.ox.cs.pdq.fol.*;

import java.time.Instant;
import java.util.Date;
import java.util.stream.IntStream;

public class NaiveDPTableSEComputationTests {
    public record RewriteResultsToBeCompared(
            DatalogProgram gsatRewriting,
            ConjunctiveQuery gsatQuery,
            DatalogRewriteResult ourRewriting,

            // the atom to which we can materialize computed homomorphisms
            Atom answerAtom
    ) {
        public FormalInstance<Constant> answersToGsatRewriting(
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

        public FormalInstance<Constant> answersToOurRewriting(
                final FormalInstance<Constant> testInstance
        ) {
            final var saturationEngine = new NaiveSaturationEngine();

            // We run the output program in two steps:
            // We first derive all facts other than subgoal/goals (i.e. with input predicates),
            // and then derive subgoal / goal facts in one go.
            // This should be much more efficient than saturating with
            // all output rules at once, since we can rather quickly saturate the base data with
            // input rules (output of GSat being relatively small, and after having done that
            // we only need to go through subgoal derivation rules once.
            final var inputRuleSaturatedInstance = saturationEngine
                    .saturateInstance(ourRewriting.inputRuleSaturationRules(), testInstance, c -> c);
            final var saturatedInstance = saturationEngine
                    .saturateInstance(ourRewriting.subgoalAndGoalDerivationRules(), inputRuleSaturatedInstance, c -> c);

            final var rewrittenGoalQuery = ConjunctiveQuery.create(
                    ourRewriting.goal().getVariables(),
                    new Atom[]{ourRewriting.goal()}
            );

            return new FormalInstance<>(
                    new FilterNestedLoopJoin<>(c -> c)
                            .join(rewrittenGoalQuery, saturatedInstance)
                            .materializeFunctionFreeAtom(answerAtom, c -> c)
            );
        }
    }

    public RewriteResultsToBeCompared rewriteInTwoMethods(
            final GTGDRuleAndGTGDReducibleQuery ruleQuery
    ) {
        System.out.println("[" + Date.from(Instant.now()) + "] Rewriting " + ruleQuery.reducibleQuery().originalQuery());

        final var gsatRewriting = DatalogProgram.tryFromDependencies(
                GSat.getInstance().run(
                        ImmutableList.<Dependency>builder()
                                .addAll(ruleQuery.guardedRules())
                                .addAll(ruleQuery.reducibleQuery().reductionRules())
                                .build()
                )
        );
        final var gsatQuery = ruleQuery.reducibleQuery().existentialFreeQuery();

        System.out.println("[" + Date.from(Instant.now()) + "] Done Gsat rewriting");

        final var ourRewriting = new GuardedRuleAndQueryRewriter(
                GSat.getInstance(),
                new NaiveDPTableSEComputation(new NaiveSaturationEngine())
        ).rewrite(ruleQuery.guardedRules(), ruleQuery.reducibleQuery().originalQuery());

        System.out.println("[" + Date.from(Instant.now()) + "] Done guarded-query rewriting");
        System.out.println("[" + Date.from(Instant.now()) + "] # of subgoal derivation rules in original output: " +
                ourRewriting.subgoalAndGoalDerivationRules().rules().size());

        final var deduplicatedFreeVariablesInQuery = ImmutableList.copyOf(ImmutableSet.copyOf(ruleQuery
                .reducibleQuery()
                .existentialFreeQuery()
                .getFreeVariables()
        ));

        final var answerAtom = Atom.create(
                Predicate.create("Answer", deduplicatedFreeVariablesInQuery.size()),
                deduplicatedFreeVariablesInQuery.toArray(Variable[]::new)
        );

        final var minimalExactBodyMinimizedRewriting = ourRewriting
                .minimizeSubgoalDerivationRulesUsing(MinimalExactBodyDatalogRuleSet::new);

        System.out.println("[" + Date.from(Instant.now()) + "] # of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " +
                minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        final var minimizedRewriting = minimalExactBodyMinimizedRewriting
                .minimizeSubgoalDerivationRulesUsing(MinimallyUnifiedDatalogRuleSet::new);

        System.out.println("[" + Date.from(Instant.now()) + "] # of subgoal derivation rules in minimizedRewriting: " +
                minimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        return new RewriteResultsToBeCompared(
                gsatRewriting,
                gsatQuery,
                minimizedRewriting,
                answerAtom
        );
    }

    private FormalInstance<Constant> allFactsOver(
            final Predicate predicate,
            final ImmutableSet<Constant> constantsToUse
    ) {
        final var predicateArgIndices = ImmutableList.copyOf(
                IntStream.range(0, predicate.getArity()).iterator()
        );

        final var allFormalFacts = MappingStreams
                .allTotalFunctionsBetween(predicateArgIndices, constantsToUse)
                .map(mapping -> new FormalFact<>(
                        predicate,
                        ImmutableList.copyOf(predicateArgIndices.stream().map(mapping::get).iterator())
                ));

        return new FormalInstance<>(allFormalFacts.iterator());
    }

    private FormalInstance<Constant> randomInstanceOver(final FunctionFreeSignature signature) {
        final var constantsToUse = ImmutableSet.copyOf(
                IntStream.range(0, signature.maxArity() * 4)
                        .<Constant>mapToObj(i -> TypedConstant.create("c_" + i))
                        .iterator()
        );

        final var allFactsOverSignature =
                signature.predicates().stream().flatMap(p -> allFactsOver(p, constantsToUse).facts.stream());

        // we first decide a selection rate and use it as a threshold
        // to filter out some of the tuples in the instance
        final var selectionRate = Math.pow(Math.random(), 2.5);

        return new FormalInstance<>(
                allFactsOverSignature
                        .filter(fact -> Math.random() < selectionRate)
                        .iterator()
        );
    }

    private void runTestFor(
            final GTGDRuleAndGTGDReducibleQuery ruleQuery,
            final int testIterationCount
    ) {
        final var rewritings = rewriteInTwoMethods(ruleQuery);

        for (int i = 0; i < testIterationCount; i++) {
            final var testInstance = randomInstanceOver(ruleQuery.signatureOfOriginalQuery());
            final var gsatAnswer = rewritings.answersToGsatRewriting(testInstance);
            final var ourAnswer = rewritings.answersToOurRewriting(testInstance);

            if (!gsatAnswer.equals(ourAnswer)) {
                throw new AssertionError(
                        "GSat and our answer differ! " +
                                "input = " + testInstance + ", " +
                                "gsatAnswer: " + gsatAnswer + ", " +
                                "ourAnswer: " + ourAnswer
                );
            } else {
                System.out.println(
                        "[" + Date.from(Instant.now()) + "] Test " + i + " passed, " +
                                "input size = " + testInstance.facts.size() + ", " +
                                "answer size = " + gsatAnswer.facts.size()
                );
            }
        }
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__atomicQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.atomicQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__joinQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.joinQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialGuardedQuery_0() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialGuardedQuery_0, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_0() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_0, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_1() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_1, 4000);
    }

    @Test
    public void testEquivalenceOn__Arity4Rule__atomicQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.Arity4Rule.atomicQuery, 30);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__atomicQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.atomicQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialBooleanQueryWithConstant() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialBooleanQueryWithConstant, 4000);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialGuardedWithConstant() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialGuardedWithConstant, 4000);
    }
}
