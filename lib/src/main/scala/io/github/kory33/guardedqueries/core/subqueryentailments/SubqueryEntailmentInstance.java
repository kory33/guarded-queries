package io.github.kory33.guardedqueries.core.subqueryentailments;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Variable;

public record SubqueryEntailmentInstance(
        ImmutableMap<Variable, Constant> ruleConstantWitnessGuess,
        ImmutableSet<Variable> coexistentialVariables,
        FormalInstance<LocalInstanceTerm> localInstance,
        ImmutableMap<Variable, LocalInstanceTerm.LocalName> localWitnessGuess,
        ImmutableBiMap<Constant, LocalInstanceTerm.LocalName /* not in the range of localWitnessGuess */> queryConstantEmbedding
) {
    public ImmutableMap<Variable, LocalInstanceTerm.RuleConstant> ruleConstantWitnessGuessAsMapToInstanceTerms() {
        return MapExtensions.composeWithFunction(
                this.ruleConstantWitnessGuess,
                LocalInstanceTerm.RuleConstant::new
        );
    }

    public SubqueryEntailmentInstance withLocalInstance(FormalInstance<LocalInstanceTerm> newLocalInstance) {
        return new SubqueryEntailmentInstance(
                this.ruleConstantWitnessGuess,
                this.coexistentialVariables,
                newLocalInstance,
                this.localWitnessGuess,
                this.queryConstantEmbedding
        );
    }
}
