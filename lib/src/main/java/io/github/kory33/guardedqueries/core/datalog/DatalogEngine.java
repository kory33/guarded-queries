package io.github.kory33.guardedqueries.core.datalog;

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;

public interface DatalogEngine {
    <TA> FormalInstance<TA> saturateInstance(final DatalogProgram program, final FormalInstance<TA> instance);

    // TODO: allow running a DatalogQuery on FormalInstance<Constant> to produce a set of results
}
