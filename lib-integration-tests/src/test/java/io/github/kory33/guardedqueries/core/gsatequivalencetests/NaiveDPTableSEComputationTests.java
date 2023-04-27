package io.github.kory33.guardedqueries.core.gsatequivalencetests;

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.NaiveDPTableSEComputation;
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases;
import io.github.kory33.guardedqueries.core.testharnesses.GSatEquivalenceTestHarness;
import org.junit.jupiter.api.Test;
import uk.ac.ox.cs.gsat.GSat;

public class NaiveDPTableSEComputationTests {
    private static final GSatEquivalenceTestHarness harness = new GSatEquivalenceTestHarness(
            GSat.getInstance(),
            new GuardedRuleAndQueryRewriter(
                    GSat.getInstance(), new NaiveDPTableSEComputation(new NaiveSaturationEngine())
            )
    );

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__atomicQuery() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.atomicQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__joinQuery() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.joinQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialGuardedQuery_0() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialGuardedQuery_0, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_0() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_0, 4000);
    }

    @Test
    public void testEquivalenceOn__SimpleArity2Rule_0__existentialJoinQuery_1() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_1, 4000);
    }

    @Test
    public void testEquivalenceOn__Arity4Rule__atomicQuery() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.Arity4Rule.atomicQuery, 30);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__atomicQuery() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.atomicQuery, 4000);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialBooleanQueryWithConstant() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialBooleanQueryWithConstant, 4000);
    }

    @Test
    public void testEquivalenceOn__ConstantRule__existentialGuardedWithConstant() {
        harness.runTestOn(GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialGuardedWithConstant, 4000);
    }
}
