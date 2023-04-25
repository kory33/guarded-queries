package io.github.kory33.guardedqueries.core.datalog;

import uk.ac.ox.cs.pdq.fol.Atom;

/**
 * A datalog program together with a specified "goal atom" appearing within the datalog program.
 * <p>
 * The goal atom should be the goal predicate applied to free variables in the query (without repetition).
 * For instance, if the input conjunctive query was {@code ∃x. R(x, y) ∧ R(y, z) ∧ S(x, z)}, then the goal atom
 * should be either {@code G(x, z)} or {@code G(z, x)}, where {@code G} is the goal predicate introduced in the
 * rewritten program.
 */
public record DatalogRewriteResult(DatalogProgram program, Atom goal) {
}
