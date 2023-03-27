package io.github.kory33.guardedqueries.core.subqueryentailments;

import uk.ac.ox.cs.pdq.fol.Constant;

public sealed interface LocalInstanceTerm {
    record LocalName(int value) implements LocalInstanceTerm {
    }

    record RuleConstant(Constant value) implements LocalInstanceTerm {
    }
}
