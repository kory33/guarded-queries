package io.github.kory33.guardedqueries.core.utils.datastructures;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public interface ImmutableStack<T> extends Iterable<T> {
    record Cons<T>(T head, ImmutableStack<T> tail) implements ImmutableStack<T> {
        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                private ImmutableStack<T> current = Cons.this;

                @Override
                public boolean hasNext() {
                    return !current.isEmpty();
                }

                @Override
                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("Nil");
                    }
                    Cons<T> currentCons = (Cons<T>) current;
                    current = currentCons.tail;
                    return currentCons.head;
                }
            };
        }

        @Override
        public String toString() {
            final var builder = new StringBuilder();
            builder.append("ImmutableStack(");
            for (T item : this) {
                builder.append(item).append(", ");
            }
            builder.delete(builder.length() - 2, builder.length());
            builder.append(")");
            return builder.toString();
        }
    }

    class Nil<T> implements ImmutableStack<T> {
        private Nil() {
        }

        // we will share this singleton instance
        private static final Nil<?> INSTANCE = new Nil<>();

        public static <T> Nil<T> getInstance() {
            //noinspection unchecked
            return (Nil<T>) INSTANCE;
        }

        @Override
        public Iterator<T> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public String toString() {
            return "ImmutableStack()";
        }
    }

    default ImmutableStack<T> push(T item) {
        return new Cons<>(item, this);
    }

    default boolean isEmpty() {
        // equivalent to "this instanceof Nil" because Nil only has one instance
        return this == empty();
    }

    default T unsafeHead() {
        if (this.isEmpty()) {
            throw new NoSuchElementException("Nil");
        }

        return ((Cons<T>) this).head;
    }

    default ImmutableStack<T> dropHead() {
        if (this.isEmpty()) {
            return empty();
        } else {
            return ((Cons<T>) this).tail;
        }
    }

    default ImmutableStack<T> replaceHeadIfNonEmpty(T item) {
        if (this.isEmpty()) {
            return empty();
        } else {
            return this.dropHead().push(item);
        }
    }

    default ImmutableStack<T> reverse() {
        // fromIterable puts the first element of the iterable at the bottom of the stack
        // and the iterator implementation of ImmutableStack traverses the stack from the top to the bottom
        return fromIterable(this);
    }

    static <T> ImmutableStack<T> fromIterable(Iterable<T> iterable) {
        ImmutableStack<T> current = Nil.getInstance();
        for (T item : iterable) {
            current = current.push(item);
        }
        return current;
    }

    @SafeVarargs
    static <T> ImmutableStack<T> of(T... items) {
        return fromIterable(Arrays.asList(items));
    }

    static <T> ImmutableStack<T> empty() {
        return Nil.getInstance();
    }
}
