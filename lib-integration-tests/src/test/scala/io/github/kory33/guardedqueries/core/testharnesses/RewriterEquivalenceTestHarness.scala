package io.github.kory33.guardedqueries.core.testharnesses

import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndConjunctiveQuery
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery
import io.github.kory33.guardedqueries.core.testharnesses.InstanceGeneration.randomInstanceOver
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.Variable

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters._

object RewriterEquivalenceTestHarness {
  private def logWithTime(message: String): Unit = {
    System.out.println(s"[${Date.from(Instant.now)}] $message")
  }

  private case class RewriteResultsToBeCompared(
    resultFromFirstRewriter: DatalogRewriteResult,
    resultFromSecondRewriter: DatalogRewriteResult,
    answerAtom: Atom
  ) {
    def answerWithFirstRewriter(testInstance: FormalInstance[Constant])
      : FormalInstance[Constant] = RunOutputDatalogProgram.answersOn(
      testInstance,
      resultFromFirstRewriter,
      answerAtom,
      (c: Constant) => c
    )

    def answerWithSecondRewriter(testInstance: FormalInstance[Constant])
      : FormalInstance[Constant] = RunOutputDatalogProgram.answersOn(
      testInstance,
      resultFromSecondRewriter,
      answerAtom,
      (c: Constant) => c
    )
  }
}

case class RewriterEquivalenceTestHarness(firstRewriter: GuardedRuleAndQueryRewriter,
                                          secondRewriter: GuardedRuleAndQueryRewriter
) {
  private def minimizeRewriteResultAndLogIntermediateCounts(
    originalRewriteResult: DatalogRewriteResult
  ) = {
    RewriterEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in original output: " + originalRewriteResult.subgoalAndGoalDerivationRules.rules.size
    )

    val minimalExactBodyMinimizedRewriting =
      originalRewriteResult.minimizeSubgoalDerivationRulesUsing(() =>
        MinimalExactBodyDatalogRuleSet()
      )

    RewriterEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " + minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules.rules.size
    )

    val minimizedRewriting =
      minimalExactBodyMinimizedRewriting.minimizeSubgoalDerivationRulesUsing(() =>
        MinimallyUnifiedDatalogRuleSet()
      )

    RewriterEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimizedRewriting: " + minimizedRewriting.subgoalAndGoalDerivationRules.rules.size
    )

    minimizedRewriting
  }

  private def rewriteUsingTwoRewriters(ruleQuery: GTGDRuleAndConjunctiveQuery) = {
    RewriterEquivalenceTestHarness.logWithTime(s"Rewriting ${ruleQuery.query}")
    RewriterEquivalenceTestHarness.logWithTime(s"Rewriting with $firstRewriter...")

    val firstRewritingStart = System.nanoTime
    val firstRewriting = firstRewriter.rewrite(ruleQuery.guardedRules, ruleQuery.query)
    RewriterEquivalenceTestHarness.logWithTime(
      s"Done rewriting with $firstRewriter in ${System.nanoTime - firstRewritingStart}ns"
    )

    val minimizedFirstRewriting = minimizeRewriteResultAndLogIntermediateCounts(firstRewriting)
    RewriterEquivalenceTestHarness.logWithTime(s"Rewriting with $secondRewriter...")
    val secondRewritingStart = System.nanoTime
    val secondRewriting = secondRewriter.rewrite(ruleQuery.guardedRules, ruleQuery.query)
    RewriterEquivalenceTestHarness.logWithTime(
      s"Done rewriting with $secondRewriter in ${System.nanoTime - secondRewritingStart}ns"
    )
    val minimizedSecondRewriting =
      minimizeRewriteResultAndLogIntermediateCounts(secondRewriting)

    val answerAtom = Atom.create(
      Predicate.create("Answer", ruleQuery.deduplicatedQueryFreeVariables.size),
      ruleQuery.deduplicatedQueryFreeVariables.toArray[Term]: _*
    )

    RewriterEquivalenceTestHarness.RewriteResultsToBeCompared(
      minimizedFirstRewriting,
      minimizedSecondRewriting,
      answerAtom
    )
  }

  /**
   * Test that the two [[GuardedRuleAndQueryRewriter]]s produce equivalent Datalog rewritings on
   * the given [[GTGDRuleAndConjunctiveQuery]].
   *
   * The test is repeatedly performed on randomly generated database instances (with the number
   * of test rounds being specified by <pre>instanceGenerationRoundCount</pre>).
   */
  def checkThatTwoRewritersAgreeOn(ruleQuery: GTGDRuleAndConjunctiveQuery,
                                   instanceGenerationRoundCount: Int
  ): Unit = {
    val rewritings = rewriteUsingTwoRewriters(ruleQuery)
    for (i <- 0 until instanceGenerationRoundCount) {
      val testInstance = randomInstanceOver(ruleQuery.signature)
      val firstAnswer = rewritings.answerWithFirstRewriter(testInstance)
      val secondAnswer = rewritings.answerWithSecondRewriter(testInstance)
      if (!(firstAnswer == secondAnswer)) throw new AssertionError(
        "Two rewriters gave different answers! " + "input = " + testInstance + ", " + "first rewriter answer = " + firstAnswer + ", " + "second rewriter answer = " + secondAnswer + ", " + "first rewriter = " + firstRewriter + ", " + "second rewriter = " + secondRewriter
      )
      else if (i % 20 == 0) RewriterEquivalenceTestHarness.logWithTime(
        "Test " + i + " passed, " + "input size = " + testInstance.facts.size + ", " + "answer size = " + firstAnswer.facts.size
      )
    }
  }

  /**
   * Test that the two [[GuardedRuleAndQueryRewriter]]s produce equivalent Datalog rewritings on
   * the given [[GTGDRuleAndGTGDReducibleQuery]].
   *
   * The test is repeatedly performed on randomly generated database instances (with the number
   * of test rounds being specified by <pre>instanceGenerationRoundCount</pre>).
   */
  def checkThatTwoRewritersAgreeOn(ruleQuery: GTGDRuleAndGTGDReducibleQuery,
                                   instanceGenerationRoundCount: Int
  ): Unit = {
    checkThatTwoRewritersAgreeOn(
      ruleQuery.asGTGDRuleAndConjunctiveQuery,
      instanceGenerationRoundCount
    )
  }
}
