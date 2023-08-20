package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.datastructures.ImmutableStack;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

public class ListExtensions {
    private ListExtensions() {
    }

    public static <I, R> Iterable<ImmutableStack<R>> productMappedCollectionsToStacks(
            final List<I> items,
            final /* pure */ Function<? super I, ? extends Iterable<R>> mapperToIterable
    ) {
        final var size = items.size();
        final var iterablesToProduct = items.stream().map(mapperToIterable).toList();
        final IntFunction<Iterator<R>> freshIteratorAt = index ->
                iterablesToProduct.get(index).iterator();

        // we store iterator of the first iterable at the bottom of the stack
        final ImmutableStack<Iterator<R>> initialIteratorStack =
                ImmutableStack.fromIterable(IntStream.range(0, size).mapToObj(freshIteratorAt).toList());

        return () -> new Iterator<>() {
            private ImmutableStack<Iterator<R>> currentIteratorStack = initialIteratorStack;

            // the next stack to be returned
            // null if no more stack of elements can be produced
            private ImmutableStack<R> currentItemStack;

            {
                currentItemStack = ImmutableStack.empty();

                // we put "the element from the iterator at the bottom of initialIteratorStack"
                // at the bottom of currentItemStack
                for (final var iterator : initialIteratorStack.reverse()) {
                    if (iterator.hasNext()) {
                        currentItemStack = currentItemStack.push(iterator.next());
                    } else {
                        // if any iterator is empty at the beginning, we cannot produce any stack of elements
                        currentItemStack = null;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return currentItemStack != null;
            }

            private void advanceState() {
                // invariant: during the loop, top `droppedIterators` have been exhausted
                //            after the loop, either the bottom iterator has been exhausted
                //            or all exhausted iterators have been replaced with fresh ones
                for (int droppedIterators = 0; droppedIterators < size; droppedIterators++) {
                    // currentIteratorStack is nonempty because it originally had `size` iterators
                    final var iteratorToAdvance = currentIteratorStack.unsafeHead();

                    if (iteratorToAdvance.hasNext()) {
                        currentItemStack = currentItemStack.replaceHeadIfNonEmpty(iteratorToAdvance.next());

                        // "restart" iterations of dropped top iterators from the beginning
                        for (int i = 0; i < droppedIterators; i++) {
                            // we dropped top iterators, which are of last iterables in iterablesToProduct
                            final var restartedIterator = freshIteratorAt.apply(size - droppedIterators + i);
                            currentIteratorStack = currentIteratorStack.push(restartedIterator);

                            // we checked in the class initializer that all fresh iterators have at least one element
                            // so as long as the mapperToIterable is pure, we can safely call next() here
                            final var firstItemFromRestartedIterator = restartedIterator.next();
                            currentItemStack = currentItemStack.push(firstItemFromRestartedIterator);
                        }
                        break;
                    } else {
                        currentIteratorStack = currentIteratorStack.dropHead();
                        currentItemStack = currentItemStack.dropHead();
                    }
                }

                // we have exhausted the bottom iterator, so we are done
                if (currentIteratorStack.isEmpty()) {
                    currentItemStack = null;
                }
            }

            @Override
            public ImmutableStack<R> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final var toReturn = currentItemStack;
                advanceState();
                return toReturn;
            }
        };
    }

    public static <I, R> Iterable<ImmutableSet<R>> productMappedCollectionsToSets(
            final List<I> items,
            final /* pure */ Function<? super I, ? extends Iterable<R>> mapperToIterable
    ) {
        return () -> IteratorExtensions.mapInto(
                productMappedCollectionsToStacks(items, mapperToIterable).iterator(),
                ImmutableSet::copyOf
        );
    }
}
