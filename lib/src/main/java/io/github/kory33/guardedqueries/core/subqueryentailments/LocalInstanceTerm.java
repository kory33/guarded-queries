package io.github.kory33.guardedqueries.core.subqueryentailments;

import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Term;

import java.util.function.Function;

public sealed interface LocalInstanceTerm {
    record LocalName(int value) implements LocalInstanceTerm {
        @Override
        public Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper) {
            return mapper.apply(this);
        }
    }

    record RuleConstant(Constant constant) implements LocalInstanceTerm {
        @Override
        public Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper) {
            return this.constant;
        }
    }

    Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper);
}
