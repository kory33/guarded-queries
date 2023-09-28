package io.github.kory33.guardedqueries.core.gsatequivalencetests

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.NaiveDPTableSEEnumeration
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases
import io.github.kory33.guardedqueries.core.testharnesses.GSatEquivalenceTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import uk.ac.ox.cs.gsat.GSat

class NaiveDPTableSEEnumerationTests extends AnyFlatSpec {
  private val harness = new GSatEquivalenceTestHarness(
    GSat.getInstance,
    GuardedRuleAndQueryRewriter(
      GSat.getInstance,
      new NaiveDPTableSEEnumeration(new NaiveSaturationEngine)
    )
  )

  "Rewriting with NaiveDPTableSEEnumeration" should "agree with GSat on SimpleArity2Rule_0.atomicQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.atomicQuery,
      4000
    )
  }

  it should "agree with GSat on SimpleArity2Rule_0.joinQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.SimpleArity2Rule_0.joinQuery,
      4000
    )
  }

  it should "agree with GSat on SimpleArity2Rule_0.existentialGuardedQuery_0" in {
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

  it should "agree with GSat on Arity4Rule.atomicQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.Arity4Rule.atomicQuery,
      /* This query, although atomic, takes about 300 ms to evaluate on average
       * partly because instance size is large (typically about 40K) due to high arity
       * in the input signature. So we only run 30 rounds.
       */
      30
    )
  }

  it should "agree with GSat on ConstantRule.atomicQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.ConstantRule.atomicQuery,
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
