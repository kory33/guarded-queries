package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.subsumption.formula.MaximallySubsumingTGDSet
import uk.ac.ox.cs.pdq.fol.Atom

/**
 * A datalog program obtained as a result of rewriting a rule set plus a conjunctive query.
 *
 * The program is decomposed into two parts: the input-rule saturation rules and the subgoal and
 * goal derivation rules. The input-rule saturation rules is only used to Datalog-saturate the
 * input database with input rules, while the subgoal and goal derivation rules is used to
 * derive the subgoal atoms and the goal atom and nothing other than that.
 *
 * The goal atom should be the goal predicate applied to free variables in the query (without
 * repetition). For instance, if the input conjunctive query was
 *
 * <pre> ∃x. R(x, y) ∧ R(y, z) ∧ S(x, z), </pre>
 *
 * then the goal atom should be either `G(x, z)` or `G(z, x)`, where `G` is the goal predicate
 * introduced in the rewritten program.
 */
case class DatalogRewriteResult(
  inputRuleSaturationRules: DatalogProgram,
  subgoalAndGoalDerivationRules: DatalogProgram,
  goal: Atom
) {

  /**
   * Optimize the set of subgoal derivation rules by removing rules that are subsumed by other
   * rules.
   *
   * It is recommended to apply multiple optimization passes, with the most efficient and the
   * coarsest (i.e. the one that concludes the least number of subsumption relations)
   * subsumption rule applied at the beginning so that we can minimize the total cost of
   * minimization and maximize the effect of the optimization.
   */
  def minimizeSubgoalDerivationRulesUsing(
    maximalDatalogRuleSetFactory: MaximallySubsumingTGDSet.Factory[
      DatalogRule,
      MaximallySubsumingTGDSet[DatalogRule]
    ]
  ): DatalogRewriteResult = {
    import scala.jdk.CollectionConverters.*

    val maximalRuleSet = maximalDatalogRuleSetFactory.emptyTGDSet()
    for (rule <- subgoalAndGoalDerivationRules.rules) {
      maximalRuleSet.addRule(rule)
    }

    DatalogRewriteResult(
      this.inputRuleSaturationRules,
      DatalogProgram(maximalRuleSet.getRules),
      this.goal
    )
  }
}
