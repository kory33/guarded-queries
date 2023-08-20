package io.github.kory33.guardedqueries.core.datalog.saturationengines;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions;
import uk.ac.ox.cs.pdq.fol.Constant;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Function;

/**
 * An implementation of {@link DatalogSaturationEngine} that performs
 * naive bottom-up saturation.
 */
public class NaiveSaturationEngine implements DatalogSaturationEngine {
    /**
     * Produce a collection of all facts that can be derived from the given set of facts
     * using the given datalog program once.
     */
    private <TA> Collection<FormalFact<TA>> chaseSingleStep(
            final DatalogProgram program,
            final ImmutableSet<FormalFact<TA>> facts,
            final Function<Constant, TA> includeConstantsToTA
    ) {
        final var inputInstance = new FormalInstance<>(facts);
        final var producedFacts = new HashSet<FormalFact<TA>>();
        final var joinAlgorithm = new FilterNestedLoopJoin<>(includeConstantsToTA);

        for (final var rule : program.rules()) {
            final var joinResult = joinAlgorithm.join(TGDExtensions.bodyAsCQ(rule), inputInstance);

            for (final var ruleHeadAtom : rule.getHeadAtoms()) {
                producedFacts.addAll(
                        // because we are dealing with Datalog rules, we can materialize every head atom
                        // using the join result (and its variable ordering)
                        joinResult.materializeFunctionFreeAtom(ruleHeadAtom, includeConstantsToTA)
                );
            }
        }

        return producedFacts;
    }

    @Override
    public <TA> FormalInstance<TA> saturateUnionOfSaturatedAndUnsaturatedInstance(
            final DatalogProgram program,
            final FormalInstance<TA> saturatedInstance,
            final FormalInstance<TA> instance,
            final Function<Constant, TA> includeConstantsToTA
    ) {
        final var saturatedFactSet = SetLikeExtensions.generateFromSetUntilFixpoint(
                SetLikeExtensions.union(saturatedInstance.facts, instance.facts),
                facts -> chaseSingleStep(program, facts, includeConstantsToTA)
        );

        return new FormalInstance<>(saturatedFactSet);
    }
}
