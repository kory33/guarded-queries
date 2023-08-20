package io.github.kory33.guardedqueries.core.subsumption.formula;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin;
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions;

import java.util.Arrays;

/**
 * An implementation of {@link MaximallySubsumingTGDSet} that keeps track of
 * a set of datalog rules which are "maximal" with respect to the following subsumption relation:
 * <p>
 * A rule R1 subsumes a rule R2 (according to this implementation) if
 * there exists a substitution {@code s} mapping variables in A to
 * variables and constants in B such that:
 * <ol>
 *     <li>{@code s(A.body)} is a subset of {@code B.body}</li>
 *     <li>{@code s(A.head)} is a superset to {@code B.head}</li>
 * </ol>
 * Note that this relation indeed implies formula implication relation A ⊨ B.
 * <p>
 * Proof:
 *   Suppose {@code A} and {@code t(B.body)} hold, where {@code t} is a substitution
 * of variables in {@code B.body} to elements in the domain of discourse.
 * As {@code s(A.body)} is a subset of {@code B.body}, {@code (t . s)(A.body)} holds.
 * By {@code A}, {@code s(A.head)} holds, and as this is a superset of {@code B.head},
 * {@code t(B.head)} holds.
 * <hr>
 * <p>
 * Such a substitution can be found by considering the body of {@code A}
 * as a conjunctive query, performing a join operation with it over the body of {@code B}
 * (considered as a formal instance of constants and variables) and then
 * materializing the head of {@code A} to check the supset condition.
 */
public final class MinimallyUnifiedDatalogRuleSet extends IndexlessMaximallySubsumingTGDSet<DatalogRule> {
    private sealed interface VariableOrConstant {
        record Variable(uk.ac.ox.cs.pdq.fol.Variable variable) implements VariableOrConstant {
        }

        record Constant(uk.ac.ox.cs.pdq.fol.Constant constant) implements VariableOrConstant {
        }

        static VariableOrConstant of(uk.ac.ox.cs.pdq.fol.Term term) {
            if (term instanceof uk.ac.ox.cs.pdq.fol.Variable v) {
                return new Variable(v);
            } else if (term instanceof uk.ac.ox.cs.pdq.fol.Constant c) {
                return new Constant(c);
            } else {
                throw new IllegalArgumentException("Either a constant or a variable is expected");
            }
        }
    }

    private static FormalFact<VariableOrConstant> atomIntoFormalFact(uk.ac.ox.cs.pdq.fol.Atom atom) {
        final var appliedTerms = ImmutableList.copyOf(Arrays
                .stream(atom.getTerms())
                .map(VariableOrConstant::of)
                .iterator()
        );

        return new FormalFact<>(atom.getPredicate(), appliedTerms);
    }

    private static FormalInstance<VariableOrConstant> atomArrayIntoFormalInstance(uk.ac.ox.cs.pdq.fol.Atom[] atoms) {
        return FormalInstance.fromIterator(
                Arrays.stream(atoms)
                        .map(MinimallyUnifiedDatalogRuleSet::atomIntoFormalFact)
                        .iterator()
        );
    }

    @Override
    protected boolean firstRuleSubsumesSecond(DatalogRule first, DatalogRule second) {
        final var joinAlgorithm = new FilterNestedLoopJoin<VariableOrConstant>(VariableOrConstant.Constant::new);

        return joinAlgorithm
                .join(TGDExtensions.bodyAsCQ(first), atomArrayIntoFormalInstance(second.getBodyAtoms()))
                .allHomomorphisms
                .stream()
                .anyMatch(homomorphism -> {
                    final var substitutedFirstHead = homomorphism.materializeFunctionFreeAtoms(
                            Arrays.asList(first.getHeadAtoms()),
                            VariableOrConstant.Constant::new
                    );
                    final var secondHead = atomArrayIntoFormalInstance(second.getHeadAtoms());

                    return substitutedFirstHead.isSuperInstanceOf(secondHead);
                });
    }
}
