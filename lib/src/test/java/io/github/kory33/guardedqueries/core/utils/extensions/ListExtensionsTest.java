package io.github.kory33.guardedqueries.core.utils.extensions;

import io.github.kory33.guardedqueries.core.utils.datastructures.ImmutableStack;
import org.apache.commons.collections4.IteratorUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

public class ListExtensionsTest {
    @Test
    public void productMappedCollectionsWithStaticSizeCollections() {
        Assertions.assertEquals(
                List.of(
                        ImmutableStack.of(1, 5, 9),
                        ImmutableStack.of(1, 5, 10),
                        ImmutableStack.of(1, 6, 9),
                        ImmutableStack.of(1, 6, 10),
                        ImmutableStack.of(2, 5, 9),
                        ImmutableStack.of(2, 5, 10),
                        ImmutableStack.of(2, 6, 9),
                        ImmutableStack.of(2, 6, 10)
                ),
                IteratorUtils.toList(
                        ListExtensions.productMappedCollectionsToStacks(
                                List.of(1, 5, 9),
                                i -> List.of(i, i + 1)
                        ).iterator()
                )
        );
    }

    @Test
    public void productMappedCollectionsReturnsCorrectlySizedStreamForLargeInput() {
        final var testCases = List.of(
                List.of(20, 10, 5, 3, 2),
                List.of(10, 5, 3, 2),
                List.of(0, 10, 5, 3, 2),
                List.of(3, 10, 5, 2, 50)
        );

        for (final var sizesOfLists : testCases) {
            final var listOfLists = sizesOfLists.stream()
                    .map(size -> IntStream.range(0, size).boxed().toList())
                    .toList();

            Assertions.assertEquals(
                    // the size of RHS is the product of the sizes of the lists in listOfLists
                    sizesOfLists.stream().reduce(1, (a, b) -> a * b).longValue(),
                    IteratorExtensions.intoStream(
                            ListExtensions.productMappedCollectionsToStacks(
                                    IntStream.range(0, listOfLists.size()).boxed().toList(),
                                    listOfLists::get
                            ).iterator()
                    ).count(),
                    "Test case: " + sizesOfLists
            );
        }
    }
}
