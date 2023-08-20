package io.github.kory33.guardedqueries.core.testharnesses;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance;
import io.github.kory33.guardedqueries.core.utils.MappingStreams;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.TypedConstant;

import java.util.stream.IntStream;

public class InstanceGeneration {
    private InstanceGeneration() {
    }

    public static FormalInstance<Constant> allFactsOver(
            final Predicate predicate,
            final ImmutableSet<Constant> constantsToUse
    ) {
        final var predicateArgIndices = ImmutableList.copyOf(
                IntStream.range(0, predicate.getArity()).iterator()
        );

        final var allFormalFacts = MappingStreams
                .allTotalFunctionsBetween(predicateArgIndices, constantsToUse)
                .map(mapping -> new FormalFact<>(
                        predicate,
                        ImmutableList.copyOf(predicateArgIndices.stream().map(mapping::get).iterator())
                ));

        return new FormalInstance<>(allFormalFacts.iterator());
    }

    public static FormalInstance<Constant> randomInstanceOver(final FunctionFreeSignature signature) {
        final var constantsToUse = ImmutableSet.copyOf(
                IntStream.range(0, signature.maxArity() * 4)
                        .<Constant>mapToObj(i -> TypedConstant.create("c_" + i))
                        .iterator()
        );

        final var allFactsOverSignature =
                signature.predicates().stream().flatMap(p -> allFactsOver(p, constantsToUse).facts.stream());

        // We first decide a selection rate and use it as a threshold
        // to filter out some of the tuples in the instance
        // We are making it more likely to select smaller instances
        // so that the answer set is usually smaller than
        // all of constantsToUse^(answer arity)
        final var selectionRate = Math.pow(Math.random(), 2.5);

        return new FormalInstance<>(
                allFactsOverSignature
                        .filter(fact -> Math.random() < selectionRate)
                        .iterator()
        );
    }

}
