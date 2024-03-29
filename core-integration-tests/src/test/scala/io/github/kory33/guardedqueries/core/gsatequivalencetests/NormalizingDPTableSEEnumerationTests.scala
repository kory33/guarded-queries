package io.github.kory33.guardedqueries.core.gsatequivalencetests

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.NormalizingDPTableSEEnumeration
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases
import io.github.kory33.guardedqueries.core.testharnesses.GSatEquivalenceTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import uk.ac.ox.cs.gsat.GSat

class NormalizingDPTableSEEnumerationTests extends AnyFlatSpec {
  private val harness = new GSatEquivalenceTestHarness(
    GSat.getInstance,
    GuardedRuleAndQueryRewriter(
      GSat.getInstance,
      new NormalizingDPTableSEEnumeration(new NaiveSaturationEngine)
    )
  )

  "Rewriting with NormalizingDPTableSEEnumeration" should "agree with GSat on SimpleArity2Rule_0.existentialGuardedQuery_0" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialGuardedQuery_0,
      4000
    )
  }

  it should "agree with GSat on SimpleArity2Rule_0.existentialJoinQuery_0" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_0,
      4000
    )
  }

  it should "agree with GSat on SimpleArity2Rule_0.existentialJoinQuery_1" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.existentialJoinQuery_1,
      4000
    )
  }

  it should "agree with GSat on ConstantRule.existentialBooleanQueryWithConstant" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialBooleanQueryWithConstant,
      4000
    )
  }

  it should "agree with GSat on ConstantRule.existentialGuardedWithConstant" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.existentialGuardedWithConstant,
      4000
    )
  }
}
