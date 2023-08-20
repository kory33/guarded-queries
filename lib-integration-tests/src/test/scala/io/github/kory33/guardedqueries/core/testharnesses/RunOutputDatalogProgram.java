package io.github.kory33.guardedqueries.core.testharnesses;

import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Constant;

import java.util.function.Function;

public class RunOutputDatalogProgram {
    private RunOutputDatalogProgram() {
    }

    /**
     * Run the given {@link DatalogRewriteResult} on the given {@link FormalInstance} and
     * materialize the result on the given {@link Atom}.
     * <p>
     * {@code answerAtom} can be any atom containing variables appearing in {@code DatalogRewriteResult.goal()},
     * and the result of running the program will be materialized on the given atom.
     * For instance, suppose that the program has two free variables {@code x} and {@code y},
     * {@code rewriteResult.goal()} is {@code G(x, y)}, and that running {@code rewriteResult} on
     * {@code testInstance} should produce answers {@code [{x <- 3, y <- 5}, {x <- 2, y <- 1}]}.
     * We can then pass {@code GoalAtom(y, x)} as {@code answerAtom} to this function to gain
     * the "materialization" of the result, which is the formal instance
     * {@code {GoalAtom(5, 3), GoalAtom(1, 2)}}.
     */
    public static <TermAlphabet> FormalInstance<TermAlphabet> answersOn(
            final FormalInstance<TermAlphabet> testInstance,
            final DatalogRewriteResult rewriteResult,
            final Atom answerAtom,
            final Function<Constant, TermAlphabet> includeConstantIntoAlphabet
    ) {
        final var saturationEngine = new NaiveSaturationEngine();

        // We run the output program in two steps:
        // We first derive all facts other than subgoal/goals (i.e. with input predicates),
        // and then derive subgoal / goal facts in one go.
        // This should be much more efficient than saturating with
        // all output rules at once, since we can rather quickly saturate the base data with
        // input rules (output of GSat being relatively small, and after having done that
        // we only need to go through subgoal derivation rules once.
        final var inputRuleSaturatedInstance = saturationEngine
                .saturateInstance(rewriteResult.inputRuleSaturationRules(), testInstance, includeConstantIntoAlphabet);
        final var saturatedInstance = saturationEngine.saturateInstance(
                rewriteResult.subgoalAndGoalDerivationRules(),
                inputRuleSaturatedInstance,
                includeConstantIntoAlphabet
        );

        final var rewrittenGoalQuery = ConjunctiveQuery.create(
                rewriteResult.goal().getVariables(),
                new Atom[]{rewriteResult.goal()}
        );

        return new FormalInstance<>(
                new FilterNestedLoopJoin<>(includeConstantIntoAlphabet)
                        .join(rewrittenGoalQuery, saturatedInstance)
                        .materializeFunctionFreeAtom(answerAtom, includeConstantIntoAlphabet)
        );
    }
}
