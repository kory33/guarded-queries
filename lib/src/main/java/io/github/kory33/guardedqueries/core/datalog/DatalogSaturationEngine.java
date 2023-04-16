package io.github.kory33.guardedqueries.core.datalog;

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import uk.ac.ox.cs.pdq.fol.Constant;

import java.util.Set;
import java.util.function.Function;

public interface DatalogSaturationEngine {
    default <TA> FormalInstance<TA> saturateInstance(
            final DatalogProgram program,
            final FormalInstance<TA> instance,
            final Function<Constant, TA> includeConstantsToTA) {
        return this.saturateUnionOfSaturatedAndUnsaturatedInstance(
                program,
                new FormalInstance<>(Set.of()),
                instance,
                includeConstantsToTA
        );
    }

    /**
     * Saturates the union of the given saturated instance and the given unsaturated instance.
     */
    <TA> FormalInstance<TA> saturateUnionOfSaturatedAndUnsaturatedInstance(
            final DatalogProgram program,
            final FormalInstance<TA> saturatedInstance,
            final FormalInstance<TA> instance,
            final Function<Constant, TA> includeConstantsToTA
    );
}
