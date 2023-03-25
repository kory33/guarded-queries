package io.github.kory33.guardedqueries.core.fol;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A GTGD in a normal form (i.e. either single-headed or full).
 * <p>
 * Any GTGD can be transformed into a pair of GTGDs in normal forms by extending the language.
 * For details, see {@link #normalize} for more details.
 */
public sealed abstract class NormalGTGD extends GTGD {
    protected NormalGTGD(final Set<Atom> body, final Set<Atom> head) {
        super(body, head);
    }

    /**
     * A single-headed GTGD.
     */
    final static class SingleHeadedGTGD extends NormalGTGD {
        public SingleHeadedGTGD(final Set<Atom> body, final Atom head) {
            super(body, Set.of(head));
        }
    }

    /**
     * An existential-free GTGD.
     */
    final static class FullGTGD extends NormalGTGD {
        /**
         * Try constructing the object from sets of atoms in body and head.
         * <p>
         * This constructor may throw an {@link IllegalArgumentException}
         * if not all variables in the head appear in the body.
         */
        public FullGTGD(final Set<Atom> body, final Set<Atom> head) {
            super(body, head);
            if (existential.length != 0) {
                throw new IllegalArgumentException(
                        "Datalog rules cannot contain existential variables, got " + super.toString()
                );
            }
        }

        /**
         * Try constructing the object from a GTGD.
         * <p>
         * This constructor may throw an {@link IllegalArgumentException}
         * if the given GTGD contains existentially quantified variables.
         */
        public static FullGTGD tryFromGTGD(final GTGD gtgd) {
            return new FullGTGD(Set.of(gtgd.getBodyAtoms()), Set.of(gtgd.getHeadAtoms()));
        }
    }

    /**
     * Normalize a collection of GTGDs.
     * <p>
     * Any GTGD can be transformed into a pair of GTGDs in normal forms by extending the language.
     * For example, a GTGD <code>∀x,y. R(x,y) -> ∃z,w. S(x,y,z) ∧ T(y,y,w)</code>
     * can be "split" into two GTGDs by introducing a fresh intermediary predicate I(-,-,-,-):
     * <ol>
     *     <li><code>∀x,y. R(x,y) -> ∃z,w. I(x,y,z,w)</code>, and</li>
     *     <li><code>∀x,y,z,w. I(x,y,z,w) → S(x,y,z) ∧ T(y,y,w)</code>.</li>
     * </ol>
     *
     * @param inputRules                  a collection of rules to normalize
     * @param intermediaryPredicatePrefix a prefix to use for naming intermediary predicates.
     *                                    For example, if the prefix is "I", intermediary predicates
     *                                    will have names "I_0", "I_1", etc.
     */
    public static ImmutableSet<NormalGTGD> normalize(
            final Collection<? extends GTGD> inputRules,
            final String intermediaryPredicatePrefix
    ) {
        final List<? extends GTGD> fullRules, existentialRules;
        {
            final var partition = inputRules
                    .stream()
                    .collect(Collectors.partitioningBy(gtgd -> gtgd.getExistential().length == 0));
            fullRules = partition.get(true);
            existentialRules = partition.get(false);
        }

        final var fullRulesStream = fullRules.stream().map(FullGTGD::tryFromGTGD);

        final Stream<NormalGTGD> splitExistentialRules;
        {
            final var frontierVariablesList = existentialRules
                    .stream()
                    .map(TGDExtensions::frontierVariables)
                    .toList();

            splitExistentialRules = IntStream
                    .range(0, existentialRules.size())
                    .boxed()
                    .flatMap(index -> {
                        final var originalRule = existentialRules.get(index);

                        final Variable[] intermediaryPredicateVariables;
                        {
                            final var frontierVariables = frontierVariablesList.get(index);
                            final var existentialVariables = Arrays.asList(originalRule.getExistential());

                            intermediaryPredicateVariables = ImmutableSet
                                    .<Variable>builder()
                                    .addAll(frontierVariables)
                                    .addAll(existentialVariables)
                                    .build()
                                    .toArray(Variable[]::new);
                        }

                        final Predicate intermediaryPredicate;
                        {
                            final var predicateName = intermediaryPredicatePrefix + "_" + Integer.toUnsignedString(index);
                            final var predicateArity = intermediaryPredicateVariables.length;
                            intermediaryPredicate = Predicate.create(predicateName, predicateArity);
                        }

                        final var splitExistentialRule = new SingleHeadedGTGD(
                                Set.of(originalRule.getBodyAtoms()),
                                Atom.create(intermediaryPredicate, intermediaryPredicateVariables)
                        );
                        final var splitFullRule = new FullGTGD(
                                Set.of(Atom.create(intermediaryPredicate, intermediaryPredicateVariables)),
                                Set.of(originalRule.getHeadAtoms())
                        );

                        return Stream.of(splitExistentialRule, splitFullRule);
                    });
        }

        final var normalizedRules = Stream.concat(fullRulesStream, splitExistentialRules);
        return ImmutableSet.copyOf(normalizedRules.toList());
    }

}
