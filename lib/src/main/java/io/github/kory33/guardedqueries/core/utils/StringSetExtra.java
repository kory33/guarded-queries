package io.github.kory33.guardedqueries.core.utils;

import java.util.Collection;

public class StringSetExtra {
    private StringSetExtra() {
    }

    public static boolean isPrefixOfSome(final Collection<String> strings, String string) {
        for (final var s : strings) {
            if (s.startsWith(string)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Pick a string starting with {@code startingWith},
     * that is not a prefix of any string from {@code strings}.
     */
    public static String freshPrefix(final Collection<String> strings, final String startingWith) {
        long count = 0;
        while (true) {
            final var candidate = startingWith + Long.toHexString(count);
            if (!isPrefixOfSome(strings, candidate)) {
                return candidate;
            }
            count += 1;
        }
    }
}
