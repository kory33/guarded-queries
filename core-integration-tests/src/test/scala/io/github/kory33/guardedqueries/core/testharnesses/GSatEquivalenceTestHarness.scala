package io.github.kory33.guardedqueries.core.testharnesses

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.datalog.{DatalogProgram, DatalogRewriteResult}
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subsumption.formula.{
  MinimalExactBodyDatalogRuleSet,
  MinimallyUnifiedDatalogRuleSet
}
import io.github.kory33.guardedqueries.core.testcases.GTGDRuleAndGTGDReducibleQuery
import io.github.kory33.guardedqueries.core.testharnesses.InstanceGeneration.randomInstanceOver
import uk.ac.ox.cs.gsat.{AbstractSaturation, GTGD}
import uk.ac.ox.cs.pdq.fol.*

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*

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
      val gsatSaturatedInstance =
        new NaiveSaturationEngine().saturateInstance(gsatRewriting, testInstance)

      FormalInstance[Constant](new FilterNestedLoopJoin[
        Variable,
        Constant
      ].joinConjunctiveQuery(
        gsatQuery,
        gsatSaturatedInstance
      ).materializeFunctionFreeAtom(answerAtom).toSet)
    }

    def answersWithOurRewriting(testInstance: FormalInstance[Constant])
      : FormalInstance[Constant] = RunOutputDatalogProgram.answersOn(
      testInstance,
      ourRewriting,
      answerAtom
    )
  }
}

/**
 * A test harness to compare the rewriting outputs of GSat and of our implementation on [[
 * GTGDRuleAndGTGDReducibleQuery]] instances.
 */
case class GSatEquivalenceTestHarness(gsatImplementation: AbstractSaturation[? <: GTGD],
                                      rewriterToBeTested: GuardedRuleAndQueryRewriter
) {
  private def minimizeRewriteResultAndLogIntermediateCounts(
    originalRewriteResult: DatalogRewriteResult
  ) = {
    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in original output: " + originalRewriteResult.subgoalAndGoalDerivationRules.size
    )

    val minimalExactBodyMinimizedRewriting =
      originalRewriteResult.minimizeSubgoalDerivationRulesUsing(() =>
        MinimalExactBodyDatalogRuleSet()
      )

    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " + minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules.size
    )

    val minimizedRewriting =
      minimalExactBodyMinimizedRewriting.minimizeSubgoalDerivationRulesUsing(() =>
        MinimallyUnifiedDatalogRuleSet()
      )

    GSatEquivalenceTestHarness.logWithTime(
      "# of subgoal derivation rules in minimizedRewriting: " + minimizedRewriting.subgoalAndGoalDerivationRules.size
    )

    minimizedRewriting
  }

  private def rewriteInTwoMethods(ruleQuery: GTGDRuleAndGTGDReducibleQuery) = {
    GSatEquivalenceTestHarness.logWithTime(
      "Rewriting " + ruleQuery.reducibleQuery.originalQuery
    )

    val gsatRewritingStart = System.nanoTime
    val gsatRewriting =
      DatalogProgram.tryFromDependencies(
        gsatImplementation.run(
          (ruleQuery.guardedRules.toList ++ ruleQuery.reducibleQuery.reductionRules).asJava
        ).asScala
      )

    GSatEquivalenceTestHarness.logWithTime(
      "Done Gsat rewriting in " + (System.nanoTime - gsatRewritingStart) + " ns"
    )

    val ourRewritingStart = System.nanoTime
    val ourRewriting =
      rewriterToBeTested.rewrite(ruleQuery.guardedRules, ruleQuery.reducibleQuery.originalQuery)

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
   * rewritings on the given [[GTGDRuleAndGTGDReducibleQuery]].
   *
   * The test is repeatedly performed on randomly generated database instances (with the number
   * of test rounds being specified by <pre>instanceGenerationRoundCount</pre>).
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
