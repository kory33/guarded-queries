package io.github.kory33.guardedqueries.core.testharnesses

import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.IncludesFolConstants
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Variable

object RunOutputDatalogProgram {

  /**
   * Run the given [[DatalogRewriteResult]] on the given [[FormalInstance]] and materialize the
   * result on the given [[Atom]].
   *
   * `answerAtom` can be any atom containing variables appearing in
   * `DatalogRewriteResult.goal()`, and the result of running the program will be materialized
   * on the given atom. For instance, suppose that the program has two free variables `x` and
   * `y`, `rewriteResult.goal()` is `G(x, y)`, and that running `rewriteResult` on
   * `testInstance` should produce answers
   *
   * <pre> [{x <- 3, y <- 5}, {x <- 2, y <- 1}]. </pre>
   *
   * We can then pass `GoalAtom(y, x)` as `answerAtom` to this function to gain the
   * "materialization" of the result, which is the formal instance
   *
   * <pre> {GoalAtom(5, 3), GoalAtom(1, 2)}. </pre>
   */
  def answersOn[TermAlphabet: IncludesFolConstants](
    testInstance: FormalInstance[TermAlphabet],
    rewriteResult: DatalogRewriteResult,
    answerAtom: Atom
  ): FormalInstance[TermAlphabet] = {
    val saturationEngine = new NaiveSaturationEngine

    /**
     * We run the output program in two steps: We first derive all facts other than
     * subgoal/goals (i.e. with input predicates), and then derive subgoal / goal facts in one
     * go.
     *
     * This should be much more efficient than saturating with all output rules at once, since
     * we can rather quickly saturate the base data with input rules (output of GSat being
     * relatively small, and after having done that we only need to go through subgoal
     * derivation rules once).
     */
    val inputRuleSaturatedInstance = saturationEngine.saturateInstance(
      rewriteResult.inputRuleSaturationRules,
      testInstance
    )
    val saturatedInstance = saturationEngine.saturateInstance(
      rewriteResult.subgoalAndGoalDerivationRules,
      inputRuleSaturatedInstance
    )

    val rewrittenGoalQuery =
      ConjunctiveQuery.create(rewriteResult.goal.getVariables, Array[Atom](rewriteResult.goal))

    FormalInstance[TermAlphabet](
      FilterNestedLoopJoin[Variable, TermAlphabet].joinConjunctiveQuery(
        rewrittenGoalQuery,
        saturatedInstance
      ).materializeFunctionFreeAtom(answerAtom).toSet
    )
  }
}
