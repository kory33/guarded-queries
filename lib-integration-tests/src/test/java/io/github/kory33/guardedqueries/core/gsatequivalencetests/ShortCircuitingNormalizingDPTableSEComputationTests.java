package io.github.kory33.guardedqueries.core.gsatequivalencetests;

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.ShortCircuitingNormalizingDPTableSEComputation;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases;
import io.github.kory33.guardedqueries.core.testharnesses.GSatEquivalenceTestHarness;
import org.junit.jupiter.api.Test;
import uk.ac.ox.cs.gsat.GSat;

public class ShortCircuitingNormalizingDPTableSEComputationTests {
    private static final GSatEquivalenceTestHarness harness = new GSatEquivalenceTestHarness(
            GSat.getInstance(),
            new GuardedRuleAndQueryRewriter(
                    GSat.getInstance(),
                    new ShortCircuitingNormalizingDPTableSEComputation(new NaiveSaturationEngine())
            )
    );

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialGuardedQuery_0() {
        harness.checkThatGSatAndTheRewriterAgreeOn(
                GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialGuardedQuery_0,
                4000
        );
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_0() {
        harness.checkThatGSatAndTheRewriterAgreeOn(
                GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_0,
                4000
        );
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_1() {
        harness.checkThatGSatAndTheRewriterAgreeOn(
                GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_1,
                4000
        );
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialBooleanQueryWithConstant() {
        harness.checkThatGSatAndTheRewriterAgreeOn(
                GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialBooleanQueryWithConstant,
                4000
        );
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialGuardedWithConstant() {
        harness.checkThatGSatAndTheRewriterAgreeOn(
                GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialGuardedWithConstant,
                4000
        );
    }
}
