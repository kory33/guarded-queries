package io.github.kory33.guardedqueries.core.gsatequivalencetests
import io.github.kory33.guardedqueries.core.datalog.reversechaseengines.NaiveReverseChaseEngine
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.NaiveReverseChaseBasedSEEnumeration
import io.github.kory33.guardedqueries.core.subsumption.localinstance.IndexlessMinimallyStrongLocalInstanceSet
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQueryTestCases
import io.github.kory33.guardedqueries.core.testharnesses.GSatEquivalenceTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import uk.ac.ox.cs.gsat.GSat

class NaiveReverseChaseBasedSEEnumerationTests extends AnyFlatSpec {
  private given NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance] =
    FilterNestedLoopJoin()

  private val harness = new GSatEquivalenceTestHarness(
    GSat.getInstance,
    GuardedRuleAndQueryRewriter(
      GSat.getInstance,
      NaiveReverseChaseBasedSEEnumeration(
        NaiveReverseChaseEngine(
          IndexlessMinimallyStrongLocalInstanceSet(_)
        ),
        IndexlessMinimallyStrongLocalInstanceSet(_)
      )
    )
  )

  "Rewriting with NaiveReverseChaseBasedSEEnumeration" should "agree with GSat on SimpleArity2Rule_0.existentialGuardedQuery_0" in {
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

  it should "agree with GSat on Arity3Rule_0.existentialJoinQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.Arity3Rule_0.existentialJoinQuery,
      // Joining is very slow with this test case
      600
    )
  }

  it should "agree with GSat on Arity3Rule_1.existentialGuardedQuery" in {
    harness.checkThatGSatAndTheRewriterAgreeOn(
      GTGDRuleAndGTGDReducibleQueryTestCases.Arity3Rule_1.existentialGuardedQuery,
      // Joining is very slow with this test case
      600
    )
  }
}
