package io.github.kory33.guardedqueries.core.datalog;

import uk.ac.ox.cs.pdq.fol.Predicate;

/**
 * A datalog program together with a specified "goal predicate".
 */
public record DatalogQuery(DatalogProgram program, Predicate goal) {
}
