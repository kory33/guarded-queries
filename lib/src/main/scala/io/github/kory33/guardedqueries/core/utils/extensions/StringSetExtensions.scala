package io.github.kory33.guardedqueries.core.utils.extensions

import scala.annotation.tailrec

object StringSetExtensions {
  given Extensions: {} with
    extension (strings: Set[String])
      def someStartsWith(string: String): Boolean = {
        strings.exists(_.startsWith(string))
      }

      /**
       * Pick a string starting with {@code prefix}, that is not a prefix of any string from
       * {@code strings}.
       */
      def freshPrefixStartingWith(prefix: String): String = {
        @tailrec def searchForFreshPrefix(count: Long): String = {
          val candidate = prefix + count.toHexString
          if (!strings.someStartsWith(candidate)) candidate
          else searchForFreshPrefix(count + 1)
        }

        searchForFreshPrefix(0)
      }
}
