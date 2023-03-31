package io.github.kory33.guardedqueries.core.subqueryentailments;

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Map;
import java.util.Set;

public record SubqueryEntailmentInstance(
        Map<Variable, Constant> ruleConstantWitnessGuess,
        Set<Variable> coexistentialVariables,
        FormalInstance<LocalInstanceTerm> localInstance,
        Map<Variable, LocalInstanceTerm.LocalName> localWitnessGuess
) {
}
