package io.github.kory33.guardedqueries.core.rewriterequivalencetests;

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.DFSNormalizingDPTableSEComputation;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.NormalizingDPTableSEComputation;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndConjunctiveQueryTestCases;
import io.github.kory33.guardedqueries.core.testharnesses.RewriterEquivalenceTestHarness;
import org.junit.jupiter.api.Test;
import uk.ac.ox.cs.gsat.GSat;

public class ShortCircuitingNormalizingAndNormalizingDPTableEquivalenceTests {
    private static final RewriterEquivalenceTestHarness harness = new RewriterEquivalenceTestHarness(
            new GuardedRuleAndQueryRewriter(
                    GSat.getInstance(),
                    new NormalizingDPTableSEComputation(new NaiveSaturationEngine())
            ),
            new GuardedRuleAndQueryRewriter(
                    GSat.getInstance(),
                    new DFSNormalizingDPTableSEComputation(new NaiveSaturationEngine())
            )
    );

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__nonReducibleJoinQuery() {
        harness.checkThatTwoRewritersAgreeOn(
                GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.nonReducibleJoinQuery,
                1000
        );
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__triangleBCQ() {
        harness.checkThatTwoRewritersAgreeOn(
                GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.triangleBCQ,
                1000
        );
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__triangleBCQWithLeaf() {
        harness.checkThatTwoRewritersAgreeOn(
                GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.triangleBCQWithLeaf,
                1000
        );
    }
}
