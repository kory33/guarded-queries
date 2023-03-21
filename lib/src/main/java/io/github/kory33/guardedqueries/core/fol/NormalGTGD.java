package io.github.kory33.guardedqueries.core.fol;

import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Atom;

import java.util.Set;

/**
 * A GTGD in normal form (i.e. either single-headed or existential-free).
 * <p>
 * Any GTGD can be transformed into a pair of GTGDs in normal forms by extending the language.
 * For example, a GTGD <code>∀x,y. R(x,y) -> ∃z,w. S(x,y,z) ∧ T(y,y,w)</code>
 * can be "split" into two GTGDs by introducing a fresh intermediary predicate I(-,-,-,-):
 * <ol>
 *     <li><code>∀x,y. R(x,y) -> ∃z,w. I(x,y,z,w)</code>, and</li>
 *     <li><code>∀x,y,z,w. I(x,y,z,w) → S(x,y,z) ∧ T(y,y,w)</code>.</li>
 * </ol>
 */
sealed abstract class NormalGTGD extends GTGD {
    protected NormalGTGD(final Set<Atom> body, final Set<Atom> head) {
        super(body, head);
    }

    /**
     * A single-headed GTGD.
     */
    final static class SingleHeaded extends NormalGTGD {
        public SingleHeaded(final Set<Atom> body, final Atom head) {
            super(body, Set.of(head));
        }
    }

    /**
     * An existential-free GTGD.
     */
    final static class ExistentialFree extends NormalGTGD {
        public ExistentialFree(final Set<Atom> body, final Set<Atom> head) {
            super(body, head);
            if (existential.length != 0) {
                throw new IllegalArgumentException(
                        "Datalog rules cannot contain existential variables, got " + super.toString()
                );
            }
        }
    }
}
