package io.github.kory33.guardedqueries.core.testharnesses

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.*

import java.time.Instant
import java.util.Date
import io.github.kory33.guardedqueries.core.testharnesses.InstanceGeneration.randomInstanceOver

object GSatEquivalenceTestHarness {
  private def logWithTime(message: String): Unit = {
    System.out.println(s"[${Date.from(Instant.now)}] $message")
  }

  private case class RewriteResultsToBeCompared(
    gsatRewriting: DatalogProgram,
    gsatQuery: ConjunctiveQuery,
    ourRewriting: DatalogRewriteResult,
    answerAtom: Atom
  ) {
    def answersWithGsatRewriting(testInstance: FormalInstance[Constant])
      : FormalInstance[Constant] = {
      val gsatSaturatedInstance = new NaiveSaturationEngine().saturateInstance(
        gsatRewriting,
        testInstance,
        (c: Constant) => c
      )
      new FormalInstance[Constant](new FilterNestedLoopJoin[Constant]((c: Constant) => c).join(
        gsatQuery,
        gsatSaturatedInstance
      ).materializeFunctionFreeAtom(answerAtom, (c: Constant) => c))
    }

    def answersWithOurRewriting(testInstance: FormalInstance[Constant])
      : FormalInstance[Constant] = RunOutputDatalogProgram.answersOn(
      testInstance,
      ourRewriting,
      answerAtom,
      (c: Constant) => c
    )
  }
}

/**
 * A test harness to compare the rewriting outputs of GSat and of our implementation on [[
 * GTGDRuleAndGTGDReducibleQuery]] instances.
 */
case class GSatEquivalenceTestHarness(gsatImplementation: AbstractSaturation[_ <: GTGD],
                                      rewriterToBeTested: GuardedRuleAndQueryRewriter
) {
  private def minimizeRewriteResultAndLogIntermediateCounts(
    originalRewriteResult: DatalogRewriteResult
  ) = {
    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in original output: " + originalRewriteResult.subgoalAndGoalDerivationRules.rules.size
    )

    val minimalExactBodyMinimizedRewriting =
      originalRewriteResult.minimizeSubgoalDerivationRulesUsing(() =>
        MinimalExactBodyDatalogRuleSet()
      )

    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " + minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules.rules.size
    )

    val minimizedRewriting =
      minimalExactBodyMinimizedRewriting.minimizeSubgoalDerivationRulesUsing(() =>
        MinimallyUnifiedDatalogRuleSet()
      )

    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimizedRewriting: " + minimizedRewriting.subgoalAndGoalDerivationRules.rules.size
    )

    minimizedRewriting
  }

  private def rewriteInTwoMethods(ruleQuery: GTGDRuleAndGTGDReducibleQuery) = {
    GSatEquivalenceTestHarness.logWithTime(
      "Rewriting " + ruleQuery.reducibleQuery.originalQuery
    )

    val gsatRewritingStart = System.nanoTime
    val gsatRewriting =
      DatalogProgram.tryFromDependencies(gsatImplementation.run(
        ImmutableList.builder[Dependency]
          .addAll(ruleQuery.guardedRules)
          .addAll(ruleQuery.reducibleQuery.reductionRules)
          .build
      ))
    GSatEquivalenceTestHarness.logWithTime(
      "Done Gsat rewriting in " + (System.nanoTime - gsatRewritingStart) + " ns"
    )

    val ourRewritingStart = System.nanoTime
    val ourRewriting =
      rewriterToBeTested.rewrite(
        ruleQuery.guardedRules,
        ruleQuery.reducibleQuery.originalQuery
      )
    GSatEquivalenceTestHarness.logWithTime(
      "Done guarded-query rewriting in " + (System.nanoTime - ourRewritingStart) + " ns"
    )

    val gsatQuery = ruleQuery.reducibleQuery.existentialFreeQuery
    val minimizedRewriting = minimizeRewriteResultAndLogIntermediateCounts(ourRewriting)
    val answerAtom = Atom.create(
      Predicate.create("Answer", ruleQuery.deduplicatedQueryFreeVariables.size),
      ruleQuery.deduplicatedQueryFreeVariables.toArray[Term]: _*
    )

    GSatEquivalenceTestHarness.RewriteResultsToBeCompared(
      gsatRewriting,
      gsatQuery,
      minimizedRewriting,
      answerAtom
    )
  }

  /**
   * Test that [[gsatImplementation]] and [[rewriterToBeTested]] produce the equivalent Datalog
   * rewritings on the given [[GTGDRuleAndGTGDReducibleQuery]]. <p> The test is repeatedly
   * performed on randomly generated database instances (with the number of test rounds being
   * specified by <pre>instanceGenerationRoundCount</pre>).
   */
  def checkThatGSatAndTheRewriterAgreeOn(
    ruleQuery: GTGDRuleAndGTGDReducibleQuery,
    instanceGenerationRoundCount: Int
  ): Unit = {
    val rewritings = rewriteInTwoMethods(ruleQuery)
    for (i <- 0 until instanceGenerationRoundCount) {
      val testInstance = randomInstanceOver(ruleQuery.signatureOfOriginalQuery)
      val gsatAnswer = rewritings.answersWithGsatRewriting(testInstance)
      val ourAnswer = rewritings.answersWithOurRewriting(testInstance)

      if (!(gsatAnswer == ourAnswer)) {
        throw new AssertionError(
          s"GSat and our answer differ! input = $testInstance, " +
            s"gsatAnswer = $gsatAnswer, ourAnswer = $ourAnswer"
        )
      } else if (i % 20 == 0) {
        GSatEquivalenceTestHarness.logWithTime(
          s"Test $i passed, input size = ${testInstance.facts.size}, answer size = ${gsatAnswer.facts.size}"
        )
      }
    }
  }
}
