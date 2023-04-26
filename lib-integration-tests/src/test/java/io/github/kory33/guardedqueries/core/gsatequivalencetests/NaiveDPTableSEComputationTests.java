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
            final var saturatedInstance = new NaiveSaturationEngine()
                    .saturateInstance(ourRewriting.program(), testInstance, c -> c);

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

        final var outRewriting = new GuardedRuleAndQueryRewriter(
                GSat.getInstance(),
                new NaiveDPTableSEComputation(new NaiveSaturationEngine())
        ).rewrite(ruleQuery.guardedRules(), ruleQuery.reducibleQuery().originalQuery());

        System.out.println("[" + Date.from(Instant.now()) + "] Done guarded-query rewriting");

        final var deduplicatedFreeVariablesInQuery = ImmutableList.copyOf(ImmutableSet.copyOf(ruleQuery
                .reducibleQuery()
                .existentialFreeQuery()
                .getFreeVariables()
        ));

        final var answerAtom = Atom.create(
                Predicate.create("Answer", deduplicatedFreeVariablesInQuery.size()),
                deduplicatedFreeVariablesInQuery.toArray(Variable[]::new)
        );

        return new RewriteResultsToBeCompared(
                gsatRewriting,
                gsatQuery,
                outRewriting,
                answerAtom
        );
    }

    private FormalInstance<Constant> randomInstanceOver(
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

        // randomly select 35% of the facts
        return new FormalInstance<>(allFormalFacts.filter(fact -> Math.random() < 0.35).iterator());
    }

    private FormalInstance<Constant> randomInstanceOver(final FunctionFreeSignature signature) {
        final var constantsToUse = ImmutableSet.copyOf(
                IntStream.range(0, signature.maxArity() * 4)
                        .<Constant>mapToObj(i -> TypedConstant.create("c_" + i))
                        .iterator()
        );

        return new FormalInstance<>(
                signature.predicates().stream()
                        .flatMap(p -> randomInstanceOver(p, constantsToUse).facts.stream())
                        .iterator()
        );
    }

    private void runTestFor(final GTGDRuleAndGTGDReducibleQuery ruleQuery) {
        final var rewritings = rewriteInTwoMethods(ruleQuery);

        for (int i = 0; i < 20; i++) {
            final var testInstance = randomInstanceOver(ruleQuery.signatureOfOriginalQuery());
            final var gsatAnswer = rewritings.answersToGsatRewriting(testInstance);
            final var ourAnswer = rewritings.answersToOurRewriting(testInstance);

            if (!gsatAnswer.equals(ourAnswer)) {
                throw new AssertionError(
                        "GSat and our answer differ, GSat answer: " + gsatAnswer + ", our answer: " + ourAnswer
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
    public void testEquivalenceOn__simpleQuery_0_atomicQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.simpleQuery_0_atomicQuery);
    }

    @Test
    public void testEquivalenceOn__simpleQuery_0_joinQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.simpleQuery_0_joinQuery);
    }

    @Test
    public void testEquivalenceOn__simpleQuery_0_existentialJoinQuery() {
        runTestFor(GTGDRuleAndGTGDReducibleQueryTestCases.simpleQuery_0_existentialJoinQuery);
    }
}
