package io.github.kory33.guardedqueries.core.datalog;

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;

import java.util.Set;

public interface DatalogSaturationEngine {
    default <TA> FormalInstance<TA> saturateInstance(final DatalogProgram program, final FormalInstance<TA> instance) {
        return this.saturateUnionOfSaturatedAndUnsaturatedInstance(
                program,
                new FormalInstance<>(Set.of()),
                instance
        );
    }

    /**
     * Saturates the union of the given saturated instance and the given unsaturated instance.
     */
    <TA> FormalInstance<TA> saturateUnionOfSaturatedAndUnsaturatedInstance(
            final DatalogProgram program,
            final FormalInstance<TA> saturatedInstance,
            final FormalInstance<TA> instance
    );
}
