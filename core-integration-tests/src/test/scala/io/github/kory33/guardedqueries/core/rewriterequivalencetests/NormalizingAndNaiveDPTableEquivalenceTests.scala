package io.github.kory33.guardedqueries.core.rewriterequivalencetests

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.{
  NaiveDPTableSEEnumeration,
  NormalizingDPTableSEEnumeration
}
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndConjunctiveQueryTestCases
import io.github.kory33.guardedqueries.core.testharnesses.RewriterEquivalenceTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import uk.ac.ox.cs.gsat.GSat

class NormalizingAndNaiveDPTableEquivalenceTests extends AnyFlatSpec {
  private val harness = new RewriterEquivalenceTestHarness(
    new GuardedRuleAndQueryRewriter(
      GSat.getInstance,
      new NaiveDPTableSEEnumeration(new NaiveSaturationEngine)
    ),
    new GuardedRuleAndQueryRewriter(
      GSat.getInstance,
      new NormalizingDPTableSEEnumeration(new NaiveSaturationEngine)
    )
  )

  "Rewritings via NaiveDPTableSEEnumeration and NormalizingDPTableSEEnumeration" should "agree on SimpleArity2Rule_0.nonReducibleJoinQuery" in {
    harness.checkThatTwoRewritersAgreeOn(
      GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.nonReducibleJoinQuery,
      1000
    )
  }

  it should "agree on SimpleArity2Rule_0.triangleBCQ" in {
    harness.checkThatTwoRewritersAgreeOn(
      GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.triangleBCQ,
      1000
    )
  }

  it should "agree on SimpleArity2Rule_0.triangleBCQWithLeaf" in {
    harness.checkThatTwoRewritersAgreeOn(
      GTGDRuleAndConjunctiveQueryTestCases.SimpleArity2Rule_0.triangleBCQWithLeaf,
      1000
    )
  }
}
