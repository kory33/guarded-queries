package io.github.kory33.guardedqueries.app

import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.{DFSNormalizingDPTableSEEnumeration, NaiveDPTableSEEnumeration, NormalizingDPTableSEEnumeration}
import io.github.kory33.guardedqueries.core.subsumption.formula
import io.github.kory33.guardedqueries.core.subsumption.formula.{MinimalExactBodyDatalogRuleSet, MinimallyUnifiedDatalogRuleSet}
import uk.ac.ox.cs.gsat.{GSat, GTGD}

object App {
  private def formatGTGD(rule: GTGD): String = {
    val headString = {
      val atomsString = rule.getHeadAtoms.map(_.toString).mkString(" ∧ ")
      val existentialVariables = rule.getExistential.toSet

      if existentialVariables.isEmpty then atomsString
      else s"∃${existentialVariables.mkString(",")}. $atomsString"
    }

    val bodyString = rule.getBodyAtoms.map(_.toString).mkString(" ∧ ")
    s"$bodyString -> $headString"
  }

  private def formatDatalogRule(rule: DatalogRule): String = {
    val headString = rule.getHeadAtoms.map(_.toString).mkString(", ")
    val bodyString = rule.getBodyAtoms.map(_.toString).mkString(", ")
    s"$headString :- $bodyString"
  }

  private def log(message: String): Unit = println(s"[guarded-queries app] $message")

  private val naiveDPRewriter = GuardedRuleAndQueryRewriter(
    GSat.getInstance(),
    NaiveDPTableSEEnumeration(NaiveSaturationEngine())
  )

  private val normalizingDPRewriter = GuardedRuleAndQueryRewriter(
    GSat.getInstance(),
    NormalizingDPTableSEEnumeration(NaiveSaturationEngine())
  )

  private val dfsNormalizingDPRewriter = GuardedRuleAndQueryRewriter(
    GSat.getInstance(),
    DFSNormalizingDPTableSEEnumeration(NaiveSaturationEngine())
  )

  private def parseCommand(line: /* non-blank */ String)
    : Either[ /* error- */ String, AppCommand] = {
    val lineWords = line.strip().split(' ')
    val commandString = lineWords.headOption match {
      case Some(c) => c
      case None    => throw IllegalArgumentException("Blank line given")
    }
    val commandArguments = lineWords.drop(1)

    commandString match
      case "show-rules"     => Right(AppCommand.ShowRegisteredRules)
      case "atomic-rewrite" => Right(AppCommand.AtomicRewriteRegisteredRules)
      case "help"           => Right(AppCommand.Help)
      case "add-rule" =>
        val ruleString = commandArguments.mkString(" ")
        val rule = AppFormulaParsers.gtgd.parse(ruleString)
        Right(AppCommand.RegisterRule(rule))
      case "rewrite" =>
        commandArguments match
          case Array(implChoiceString, formulaStrings: _*) =>
            val implChoice = implChoiceString match
              case "naive"           => AppCommand.EnumerationImplChoice.Naive
              case "normalizing"     => AppCommand.EnumerationImplChoice.Normalizing
              case "dfs-normalizing" => AppCommand.EnumerationImplChoice.DFSNormalizing
              case _ => return Left(s"Invalid rewrite implementation choice: $implChoiceString")

            val formulaString = formulaStrings.mkString(" ")
            val formula = AppFormulaParsers.conjunctiveQuery.parse(formulaString)

            Right(AppCommand.Rewrite(formula, implChoice))

          case _ => Left("Not enough arguments for rewrite command")
      case c => Left(s"Invalid command: $c")
  }

  private def minimizeRewriteResultAndLogIntermediateCounts(
    originalRewriteResult: DatalogRewriteResult
  ): DatalogRewriteResult = {
    log {
      "# of subgoal derivation rules in original output: " +
        originalRewriteResult.subgoalAndGoalDerivationRules.size
    }

    val minimalExactBodyMinimizedRewriting =
      originalRewriteResult
        .minimizeSubgoalDerivationRulesUsing(() => MinimalExactBodyDatalogRuleSet())

    log {
      "# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " +
        minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules.size
    }

    val minimizedRewriting =
      minimalExactBodyMinimizedRewriting
        .minimizeSubgoalDerivationRulesUsing(() => MinimallyUnifiedDatalogRuleSet())

    log {
      "# of subgoal derivation rules in minimizedRewriting: " +
        minimizedRewriting.subgoalAndGoalDerivationRules.size
    }

    minimizedRewriting
  }

  private def runCommandAndObtainNewAppState(currentState: AppState,
                                             command: AppCommand
  ): AppState = {
    import scala.jdk.CollectionConverters.*

    command match
      case AppCommand.RegisterRule(rule) =>
        log("Registered rule: " + formatGTGD(rule))
        currentState.registerRule(rule)
      case AppCommand.ShowRegisteredRules =>
        log("Registered rules:")
        currentState.registeredRules.foreach(rule => log("  " + formatGTGD(rule)))
        currentState
      case AppCommand.AtomicRewriteRegisteredRules =>
        log("Computing atomic rewriting of registered rules.")
        log("Registered rules:")
        currentState.registeredRules.foreach(rule => log("  " + formatGTGD(rule)))

        val rewrittenRules =
          GSat.getInstance().run(currentState.registeredRulesAsDependencies.asJava)
        log("Done computing atomic rewriting of registered rules")
        log("Rewritten rules:")
        rewrittenRules.asScala.foreach(rule => log("  " + formatGTGD(rule)))
        currentState
      case AppCommand.Rewrite(query, implChoice) =>
        val rewriter = implChoice match
          case AppCommand.EnumerationImplChoice.Naive          => naiveDPRewriter
          case AppCommand.EnumerationImplChoice.Normalizing    => normalizingDPRewriter
          case AppCommand.EnumerationImplChoice.DFSNormalizing => dfsNormalizingDPRewriter

        log("Rewriting query:" + query)
        log("  using " + rewriter + ", with registered rules:")
        currentState.registeredRules.foreach(rule => log("  " + formatGTGD(rule)))
        val beginRewriteNanoTime = System.nanoTime()
        val rewriteResult = rewriter.rewrite(currentState.registeredRules, query)
        val rewriteTimeNanos = System.nanoTime() - beginRewriteNanoTime
        log("Done rewriting query in " + rewriteTimeNanos + " nanoseconds.")
        log("Minimizing the result...")

        val beginMinimizeNanoTime = System.nanoTime()
        val minimizedRewriteResult =
          minimizeRewriteResultAndLogIntermediateCounts(rewriteResult)
        val minimizeTimeNanos = System.nanoTime() - beginMinimizeNanoTime
        log("Done minimizing the result in " + minimizeTimeNanos + " nanoseconds.")

        log("Rewritten query:")
        log("  Goal atom: " + minimizedRewriteResult.goal)
        log("  Atomic rewriting part:")
        minimizedRewriteResult.inputRuleSaturationRules.foreach(rule =>
          log("    " + formatDatalogRule(rule))
        )
        log("  Subgoal derivation part:")
        minimizedRewriteResult.subgoalAndGoalDerivationRules.foreach(rule =>
          log("    " + formatDatalogRule(rule))
        )

        currentState
      case AppCommand.Help =>
        log("Available commands:")
        log("  add-rule <rule>")
        log("  show-rules")
        log("  atomic-rewrite")
        log("  rewrite <naive|normalizing|dfs-normalizing> <query>")
        log("  help")
        log("  exit")

        currentState
  }

  def main(args: Array[String]): Unit = {
    var appState = AppState.empty
    val scanner = java.util.Scanner(System.in)

    while (true) {
      println("[guarded-queries app] > ")
      val nextLine = scanner.nextLine()

      if nextLine.strip() == "exit" then
        return
      else if !nextLine.isBlank then
        parseCommand(nextLine) match
          case Right(command) =>
            try appState = runCommandAndObtainNewAppState(appState, command)
            catch case e: Exception => println(s"Error: ${e.getMessage}")
          case Left(error) => println(error)
    }
  }
}
