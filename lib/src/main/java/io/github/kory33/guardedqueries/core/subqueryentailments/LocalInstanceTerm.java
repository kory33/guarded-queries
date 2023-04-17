package io.github.kory33.guardedqueries.core.subqueryentailments;

import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Term;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.function.Function;

public sealed interface LocalInstanceTerm {
    record LocalName(int value) implements LocalInstanceTerm {
        @Override
        public Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper) {
            return mapper.apply(this);
        }

        @Override
        public String toString() {
            return "LocalName[" + value + "]";
        }
    }

    record RuleConstant(Constant constant) implements LocalInstanceTerm {
        @Override
        public Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper) {
            return this.constant;
        }

        @Override
        public String toString() {
            return "RuleConstant[" + constant + "]";
        }
    }

    Term mapLocalNamesToTerm(Function<? super LocalName, ? extends Term> mapper);

    static LocalInstanceTerm fromTermWithVariableMap(
            final Term term,
            final Function<? super Variable, ? extends LocalInstanceTerm> mapper
    ) {
        if (term instanceof Constant constant) {
            return new RuleConstant(constant);
        } else if (term instanceof Variable variable) {
            return mapper.apply(variable);
        } else {
            throw new IllegalArgumentException("Unsupported term: " + term);
        }
    }
}
